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
        range: { min: -0.005, max: 0.01 },
    },
    "baratron_gauge_i": { label: "Baratron Gauge", color: "#ff6384" },
    "ar_mfc_i": { label: "Ar MFC", color: "#36a2eb" },
};

const charts = {};
const axisStates = {};
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

const colorPalette = [
    '#4bc0c0', '#ffcd56', '#36a2eb', '#9966ff', '#2ecc71', '#f39c12', '#8e44ad',
    '#1abc9c', '#2c3e50', '#7f8c8d', '#9b59b6', '#3498db', '#ff6384', '#e74c3c'
];

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
        chart.data.datasets[0].data = [];
    });
    resetAxisStates();
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
            lineData.push({ x: timeValue.getTime(), y: numericValue });
            if (lineData.length > MAX_POINTS) {
                lineData.splice(0, lineData.length - MAX_POINTS);
            }
            const cutoffTime = lineData.length > 0 ? lineData[0].x : null;
            if (cutoffTime !== null && anomalyData.length > 0) {
                const filtered = anomalyData.filter((point) => point.x >= cutoffTime);
                anomalyData.splice(0, anomalyData.length, ...filtered);
            }
            if (abnormalFields.includes(column)) {
                anomalyData.push({ x: timeValue.getTime(), y: numericValue });
            }
            ensureAxisState(column, numericValue);
        });
    });

    Object.values(charts).forEach((chart) => chart.update('none'));
    lastTimestamp = rows[rows.length - 1].timer;
    if (lastUpdatedEl) {
        lastUpdatedEl.textContent = new Date().toLocaleString('ko-KR');
    }
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

    try {
        const response = await fetch(url, { cache: 'no-store' });
        if (!response.ok) {
            throw new Error(`데이터 요청 실패: ${response.status}`);
        }
        const data = await response.json();
        if (errorBannerEl) {
            errorBannerEl.style.display = 'none';
            errorBannerEl.textContent = '';
        }

        if (!data || !data.table) {
            if (currentTableEl) {
                currentTableEl.textContent = '-';
            }
            return;
        }

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
    } catch (error) {
        console.error(error);
        if (errorBannerEl) {
            errorBannerEl.style.display = 'block';
            errorBannerEl.textContent = '데이터를 불러오지 못했습니다. 잠시 후 다시 시도합니다.';
        }
    }
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

chartContainers.forEach((container, index) => createChart(container, index));
if (chartContainers.length > 0) {
    startPolling();
}