const columnMeta = {
    "Ion.Gauge.i": {
        label: "Ion Gauge",
        color: "#4bc0c0",
        range: { min: 0, max: 0.01 },
    },
    "Baratron.Gauge.i": { label: "Baratron Gauge", color: "#ff6384" },
    "Ar.MFC.i": { label: "Ar MFC", color: "#36a2eb" },
    "Line.Gauge.i": { label: "Line Gauge", color: "#9966ff" },
    "Stage1.Temp1": { label: "Stage1 Temp1", color: "#ff9f40" },
    "Stage2.Temp1": { label: "Stage2 Temp1", color: "#2ecc71" },
    "Ar.MFC.o": { label: "Ar MFC Out", color: "#e74c3c" },
    "Power": { label: "Power", color: "#f39c12" },
    "Current": { label: "Current", color: "#8e44ad" },
    "Volt": { label: "Volt", color: "#1abc9c" },
    "ion_gauge_i": {
        label: "Ion Gauge",
        color: "#4bc0c0",
        range: { min: -0.001, max: 0.01 },
    },
    "baratron_gauge_i": { label: "Baratron Gauge", color: "#ffcd56" },
    "ar_mfc_i": { label: "Ar MFC", color: "#36a2eb" },
};

const charts = {};
const axisStates = {};
const timeBounds = { min: null, max: null };
const LOCK_AFTER_POINTS = 10;
const MIN_LOCK_SPAN = 1;
const MAX_POINTS = 3600; // 1시간 분량 (1Hz)

let lastTable = null;
let lastTimestamp = null;
let pollingHandle = null;

const chartContainers = Array.from(document.querySelectorAll('.chart-container'));
const columnOrder = chartContainers.map((container) => container.dataset.column);

const currentTableEl = document.getElementById('current-table');
const lastUpdatedEl = document.getElementById('last-updated');
const errorBannerEl = document.getElementById('error-banner');
const chartsSectionEl = document.getElementById('charts-container');
const logPanelEl = document.getElementById('log-panel');
const logListEl = document.getElementById('log-list');
const logErrorEl = document.getElementById('log-error');

const colorPalette = [
    '#4bc0c0', '#ffcd56', '#36a2eb', '#9966ff', '#2ecc71', '#f39c12', '#8e44ad',
    '#1abc9c', '#2c3e50', '#7f8c8d', '#9b59b6', '#3498db', '#ff6384', '#e74c3c'
];

const LOG_LIMIT = 50;
const logAnomalies = new Map();
let panelHeightFrame = null;

function syncPanelHeights() {
    if (!chartsSectionEl || !logPanelEl) {
        return;
    }

    const previousChartHeight = chartsSectionEl.style.height;
    const previousLogHeight = logPanelEl.style.height;

    chartsSectionEl.style.height = 'auto';
    logPanelEl.style.height = 'auto';

    const measuredHeight = Math.ceil(chartsSectionEl.getBoundingClientRect().height);

    if (Number.isFinite(measuredHeight) && measuredHeight > 0) {
        chartsSectionEl.style.height = `${measuredHeight}px`;
        logPanelEl.style.height = `${measuredHeight}px`;
    } else {
        chartsSectionEl.style.height = previousChartHeight;
        logPanelEl.style.height = previousLogHeight;
    }
}

function schedulePanelHeightSync() {
    if (panelHeightFrame !== null) {
        return;
    }
    panelHeightFrame = requestAnimationFrame(() => {
        panelHeightFrame = null;
        syncPanelHeights();
    });
}

function normalizeFieldName(field) {
    if (typeof field !== 'string') {
        return null;
    }
    const trimmed = field.trim();
    if (!trimmed) {
        return null;
    }
    if (columnMeta[trimmed]) {
        // 이미 정의된 메타 키이면 그대로 사용
        return trimmed;
    }
    return trimmed
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function updateLogAnomalyMap(logs, activeTable) {
    logAnomalies.clear();
    if (!Array.isArray(logs)) {
        return;
    }

    logs.forEach((log) => {
        if (!log || !log.timer) {
            return;
        }

        if (activeTable && log.source_table && log.source_table !== activeTable) {
            return;
        }

        const date = new Date(log.timer);
        const timestamp = date.getTime();
        if (Number.isNaN(timestamp)) {
            return;
        }

        const bucket = Math.floor(timestamp / 1000) * 1000;
        const fields = Array.isArray(log.fields) ? log.fields : [];
        fields.forEach((field) => {
            const normalized = normalizeFieldName(field);
            if (!normalized) {
                return;
            }
            if (!logAnomalies.has(bucket)) {
                logAnomalies.set(bucket, new Set());
            }
            logAnomalies.get(bucket).add(normalized);
        });
    });
}

function getLogAnomaliesFor(timestamp) {
    if (!Number.isFinite(timestamp)) {
        return undefined;
    }
    const bucket = Math.floor(timestamp / 1000) * 1000;
    return logAnomalies.get(bucket);
}

function applyLogAnomaliesToCharts() {
    Object.entries(charts).forEach(([column, chart]) => {
        if (!chart || !chart.data || !chart.data.datasets) {
            return;
        }
        const lineData = chart.data.datasets[0]?.data;
        const anomalyData = chart.data.datasets[1]?.data;
        if (!Array.isArray(lineData) || !Array.isArray(anomalyData)) {
            return;
        }

        anomalyData.length = 0;
        lineData.forEach((point) => {
            if (!point || !Number.isFinite(point.x)) {
                return;
            }
            const logFields = getLogAnomaliesFor(point.x);
            if (logFields && logFields.has(column)) {
                point.abnormal = true;
            }
            if (point.abnormal) {
                anomalyData.push({ x: point.x, y: point.y });
            }
        });

        chart.update('none');
    });
}

function hideLogError() {
    if (logErrorEl) {
        logErrorEl.style.display = 'none';
        logErrorEl.textContent = '';
    }
}

function showLogError(message) {
    if (logErrorEl) {
        logErrorEl.style.display = 'block';
        logErrorEl.textContent = message;
    }
}

function formatTimestamp(value) {
    if (!value) {
        return '-';
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return value;
    }
    return date.toLocaleString('ko-KR', { hour12: false });
}

function renderLogs(logs) {
    if (!logListEl) {
        return;
    }

    logListEl.innerHTML = '';

    if (!logs || logs.length === 0) {
        const emptyEl = document.createElement('div');
        emptyEl.className = 'log-empty';
        emptyEl.textContent = '최근 이상 로그가 없습니다.';
        logListEl.appendChild(emptyEl);
        schedulePanelHeightSync();
        return;
    }

    logs.forEach((log) => {
        const entry = document.createElement('div');
        entry.className = 'log-entry';

        const timeEl = document.createElement('div');
        timeEl.className = 'log-time';
        const primaryTime = log.timer || log.created_at;
        const secondary = log.created_at && log.created_at !== primaryTime
            ? `기록: ${formatTimestamp(log.created_at)}`
            : '';
        const times = [formatTimestamp(primaryTime)];
        if (secondary) {
            times.push(secondary);
        }
        timeEl.textContent = times.filter(Boolean).join(' | ');
        entry.appendChild(timeEl);

        if (Array.isArray(log.fields) && log.fields.length > 0) {
            const fieldsEl = document.createElement('div');
            fieldsEl.className = 'log-fields';
            const fieldLabels = log.fields.map((field) => {
                const label = columnMeta[field]?.label || field;
                return label === field ? field : `${label} (${field})`;
            });
            fieldsEl.textContent = `컬럼: ${fieldLabels.join(', ')}`;
            entry.appendChild(fieldsEl);
        }

        const textEl = document.createElement('div');
        textEl.className = 'log-text';
        textEl.textContent = log.log_text || '';
        entry.appendChild(textEl);

        logListEl.appendChild(entry);
    });

    logListEl.scrollTop = 0;
    schedulePanelHeightSync();
}

function pickColor(index, fallback) {
    if (fallback) {
        return fallback;
    }
    return colorPalette[index % colorPalette.length];
}

function ensureAxisState(column, value) {
    if (!Number.isFinite(value)) {
        return;
    }

    const chart = charts[column];
    if (!chart) {
        return;
    }

    const meta = columnMeta[column] || {};
    if (meta.range && Number.isFinite(meta.range.min) && Number.isFinite(meta.range.max)) {
        chart.options.scales.y.min = meta.range.min;
        chart.options.scales.y.max = meta.range.max;
        return;
    }

    let state = axisStates[column];
    if (!state) {
        state = {
            count: 0,
            dataMin: value,
            dataMax: value,
            locked: false,
            min: value,
            max: value,
        };
        axisStates[column] = state;
    }

    if (!state.locked) {
        state.count += 1;
        if (value < state.dataMin) {
            state.dataMin = value;
        }
        if (value > state.dataMax) {
            state.dataMax = value;
        }

        const rawSpan = state.dataMax - state.dataMin;
        const padding = rawSpan === 0
            ? Math.max(Math.abs(state.dataMax) * 0.2, 1)
            : Math.max(rawSpan * 0.2, 1e-6);
        const span = Math.max(rawSpan + padding * 2, MIN_LOCK_SPAN);
        const center = (state.dataMax + state.dataMin) / 2;

        state.min = center - span / 2;
        state.max = center + span / 2;
        state.span = span;

        if (state.count >= LOCK_AFTER_POINTS) {
            state.locked = true;
        }
    } else {
        const span = state.span;
        if (value > state.max) {
            state.max = value;
            state.min = value - span;
            state.span = span;
        } else if (value < state.min) {
            state.min = value;
            state.max = value + span;
            state.span = span;
        }
    }

    chart.options.scales.y.min = state.min;
    chart.options.scales.y.max = state.max;
}

function resetAxisStates() {
    Object.keys(axisStates).forEach((key) => delete axisStates[key]);
    Object.entries(charts).forEach(([column, chart]) => {
        const meta = columnMeta[column] || {};
        if (meta.range && Number.isFinite(meta.range.min) && Number.isFinite(meta.range.max)) {
            chart.options.scales.y.min = meta.range.min;
            chart.options.scales.y.max = meta.range.max;
        } else {
            chart.options.scales.y.min = undefined;
            chart.options.scales.y.max = undefined;
        }
        chart.update('none');
    });
}

function createChart(container, index) {
    const column = container.dataset.column;
    const meta = columnMeta[column] || {};
    const canvas = container.querySelector('canvas');
    if (!canvas) {
        return;
    }

    const ctx = canvas.getContext('2d');
    const displayLabel = meta.label || column;
    const color = pickColor(index, meta.color);
    const range = meta.range;

    charts[column] = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: displayLabel,
                    data: [],
                    borderColor: color,
                    backgroundColor: color,
                    tension: 0.2,
                    pointRadius: 0,
                    borderWidth: 4,
                    segment: {
                        borderColor: (ctx) => {
                            const isAbnormal =
                                (ctx?.p0?.raw && ctx.p0.raw.abnormal) ||
                                (ctx?.p1?.raw && ctx.p1.raw.abnormal);
                            return isAbnormal ? '#e74c3c' : color;
                        },
                    },
                },
                {
                    label: `${displayLabel} 이상`,
                    data: [],
                    type: 'scatter',
                    showLine: false,
                    backgroundColor: '#e74c3c',
                    borderColor: '#ffffff',
                    borderWidth: 1,
                    pointRadius: 4,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#e74c3c',
                    pointBorderColor: '#ffffff',
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            parsing: false,
            interaction: {
                intersect: false,
                mode: 'nearest'
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                        displayFormats: {
                            second: 'HH:mm:ss',
                            minute: 'HH:mm',
                        }
                    },
                    ticks: {
                        source: 'data',
                        autoSkip: true,
                        maxRotation: 0,
                        color: '#495057'
                    },
                    grid: {
                        display: false
                    }
                },
                y: {
                    ticks: {
                        color: '#495057'
                    },
                    grid: {
                        color: 'rgba(0,0,0,0.08)'
                    },
                    min: range && Number.isFinite(range.min) ? range.min : undefined,
                    max: range && Number.isFinite(range.max) ? range.max : undefined,
                }
            },
            plugins: {
                legend: {
                    display: false
                }
            }
        }
    });
}

function clearCharts() {
    Object.values(charts).forEach((chart) => {
        chart.data.datasets.forEach((dataset) => {
            dataset.data = [];
        });
        chart.options.scales.x.min = undefined;
        chart.options.scales.x.max = undefined;
    });
    timeBounds.min = null;
    timeBounds.max = null;
    resetAxisStates();
    logAnomalies.clear();
}

function syncTimeScales() {
    let min = null;
    let max = null;

    Object.values(charts).forEach((chart) => {
        const data = chart.data.datasets[0]?.data;
        if (!data || data.length === 0) {
            return;
        }
        const first = data[0].x;
        const last = data[data.length - 1].x;
        min = min === null ? first : Math.min(min, first);
        max = max === null ? last : Math.max(max, last);
    });

    if (min === null || max === null) {
        timeBounds.min = null;
        timeBounds.max = null;
        Object.values(charts).forEach((chart) => {
            chart.options.scales.x.min = undefined;
            chart.options.scales.x.max = undefined;
            chart.update('none');
        });
        return;
    }

    if (timeBounds.min !== min || timeBounds.max !== max) {
        timeBounds.min = min;
        timeBounds.max = max;
        Object.values(charts).forEach((chart) => {
            chart.options.scales.x.min = min;
            chart.options.scales.x.max = max;
        });
    }
}

function appendRows(rows) {
    if (!rows || rows.length === 0) {
        return;
    }

    rows.forEach((row) => {
        if (!row.timer) {
            return;
        }
        const timeValue = new Date(row.timer);
        const abnormalFields = Array.isArray(row.abnormal_fields) ? row.abnormal_fields : [];
        const normalizedFields = new Set(
            abnormalFields
                .map((field) => normalizeFieldName(field))
                .filter((field) => typeof field === 'string' && field.length > 0),
        );
        columnOrder.forEach((column) => {
            const value = row[column];
            if (value === null || value === undefined || Number.isNaN(Number(value))) {
                return;
            }
            const chart = charts[column];
            if (!chart) {
                return;
            }
            const numericValue = Number(value);
            const lineData = chart.data.datasets[0].data;
            const anomalyData = chart.data.datasets[1].data;
            const timestamp = timeValue.getTime();
            let isAbnormal = normalizedFields.has(column);
            if (!isAbnormal) {
                const logFields = getLogAnomaliesFor(timestamp);
                isAbnormal = Boolean(logFields && logFields.has(column));
            }

            const lastPoint = lineData[lineData.length - 1];
            if (lastPoint && lastPoint.x === timestamp) {
                lastPoint.y = numericValue;
                lastPoint.abnormal = isAbnormal;
            } else {
                lineData.push({ x: timestamp, y: numericValue, abnormal: isAbnormal });
            }
            if (lineData.length > MAX_POINTS) {
                lineData.splice(0, lineData.length - MAX_POINTS);
            }
            const cutoffTime = lineData.length > 0 ? lineData[0].x : null;
            if (cutoffTime !== null && anomalyData.length > 0) {
                const filtered = anomalyData.filter((point) => point.x >= cutoffTime);
                anomalyData.splice(0, anomalyData.length, ...filtered);
            }
            if (isAbnormal) {
                const lastAnomaly = anomalyData[anomalyData.length - 1];
                if (lastAnomaly && lastAnomaly.x === timestamp) {
                    lastAnomaly.y = numericValue;
                } else {
                    anomalyData.push({ x: timestamp, y: numericValue });
                }
            }
            ensureAxisState(column, numericValue);
        });
    });

    syncTimeScales();
    Object.values(charts).forEach((chart) => chart.update('none'));
    lastTimestamp = rows[rows.length - 1].timer;
    if (lastUpdatedEl) {
        lastUpdatedEl.textContent = new Date().toLocaleString('ko-KR');
    }
    schedulePanelHeightSync();
}

async function fetchLatest() {
    const params = new URLSearchParams();
    if (lastTable) {
        params.append('last_table', lastTable);
    }
    if (lastTimestamp) {
        params.append('since', lastTimestamp);
    }

    const url = params.toString() ? `/api/pvd/latest?${params.toString()}` : '/api/pvd/latest';
    const logUrl = `/api/pvd/logs?limit=${LOG_LIMIT}`;

    const [latestResult, logsResult] = await Promise.allSettled([
        fetch(url, { cache: 'no-store' }).then((res) => {
            if (!res.ok) {
                throw new Error(`데이터 요청 실패: ${res.status}`);
            }
            return res.json();
        }),
        fetch(logUrl, { cache: 'no-store' }).then((res) => {
            if (!res.ok) {
                throw new Error(`로그 요청 실패: ${res.status}`);
            }
            return res.json();
        }),
    ]);

    if (latestResult.status === 'fulfilled') {
        const data = latestResult.value;
        if (errorBannerEl) {
            errorBannerEl.style.display = 'none';
            errorBannerEl.textContent = '';
        }

        if (!data || !data.table) {
            if (currentTableEl) {
                currentTableEl.textContent = '-';
            }
        } else {
            if (currentTableEl) {
                currentTableEl.textContent = data.table;
            }

            if (data.is_new_table || lastTable !== data.table) {
                lastTable = data.table;
                lastTimestamp = null;
                clearCharts();
            }

            if (Array.isArray(data.rows) && data.rows.length > 0) {
                appendRows(data.rows);
            }
        }
    } else {
        console.error(latestResult.reason);
        if (errorBannerEl) {
            errorBannerEl.style.display = 'block';
            errorBannerEl.textContent = '데이터를 불러오지 못했습니다. 잠시 후 다시 시도합니다.';
        }
    }

    if (logsResult.status === 'fulfilled') {
        hideLogError();
        updateLogAnomalyMap(logsResult.value, lastTable);
        renderLogs(logsResult.value);
        applyLogAnomaliesToCharts();
    } else {
        console.error(logsResult.reason);
        showLogError('로그를 불러오지 못했습니다. 잠시 후 다시 시도합니다.');
    }

    schedulePanelHeightSync();
}

function startPolling() {
    if (pollingHandle) {
        clearInterval(pollingHandle);
    }
    fetchLatest();
    pollingHandle = setInterval(fetchLatest, 1000);
}

window.addEventListener('beforeunload', () => {
    if (pollingHandle) {
        clearInterval(pollingHandle);
    }
});

window.addEventListener('resize', schedulePanelHeightSync);

chartContainers.forEach((container, index) => createChart(container, index));
if (chartContainers.length > 0) {
    startPolling();
}

schedulePanelHeightSync();