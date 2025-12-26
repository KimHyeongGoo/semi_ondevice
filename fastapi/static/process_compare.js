let predictColumns = [];
let charts = new Map();
let logsByParameter = new Map();
let currentRequestId = 0;
let selectedParams = new Set();
let activeCategories = new Set();
const chartCards = new Map();
const selectedLogKeys = new Set();
const logElements = new Map();
const highlightRegionsByParam = new Map();
let stepFilterValue = 'all';
let sortOrder = 'desc';
const displayParam = (name) => {
    const m = String(name || '').match(/^MFC\\d+[_ ]?(.*)$/i);
    return m && m[1] ? m[1] : name;
};

const CATEGORY_GROUPS = {
    MFC: [
        'MFC7_DCS',
        'MFC8_NH3',
        'MFC1_N2-1',
        'MFC2_N2-2',
        'MFC3_N2-3',
        'MFC4_N2-4',
    ],
    Pressure: [
        'VG11 Press value',
        'VG12 Press value',
        'VG13 Press value',
    ],
    Temperature: [
        'Temp_Act_U',
        'Temp_Act_CU',
        'Temp_Act_C',
        'Temp_Act_CL',
        'Temp_Act_L',
    ],
    Actuator: [
        'MFC26_F.PWR',
        'MFC27_L.POS',
        'MFC28_R.POS',
    ],
};

const selectionHighlightPlugin = {
    id: 'selectionHighlight',
    afterDatasetsDraw(chart, args, opts) {
        const param = chart._codexParam;
        if (!param) return;
        const regions = opts?.getRegions ? opts.getRegions(param) : [];
        if (!regions || !regions.length) return;
        const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
        ctx.save();
        ctx.fillStyle = 'rgba(0, 128, 0, 0.28)';
        regions.forEach(region => {
            const xStart = x.getPixelForValue(region.start);
            const xEnd = x.getPixelForValue(region.end);
            ctx.fillRect(xStart, top, xEnd - xStart, bottom - top);
        });
        ctx.restore();
    }
};
Chart.register(selectionHighlightPlugin);

function safeId(text) {
    return text.replace(/[^a-zA-Z0-9]/g, '_');
}

function formatLocal(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function formatFullKorean(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    const pad = (n) => String(n).padStart(2, '0');
    const y = d.getFullYear();
    const m = pad(d.getMonth() + 1);
    const day = pad(d.getDate());
    const h = pad(d.getHours());
    const min = pad(d.getMinutes());
    const s = pad(d.getSeconds());
    return `${y}년 ${m}월 ${day}일 ${h}시 ${min}분 ${s}초`;
}

function parseLogMessage(message) {
    if (!message) return null;
    try {
        return JSON.parse(message);
    } catch (e) {
        return null;
    }
}

function buildLogDescription(log) {
    const parsed = parseLogMessage(log.message);
    const param = parsed?.parameter || log.parameter || '';
    const label = displayParam(param);
    const diffVal = parsed?.diff_percent ?? parsed?.diff;
    const actualVal = parsed?.actual_value;
    const predictedVal = parsed?.predicted_value;
    const diffRaw = Number.isFinite(Number(diffVal)) ? Math.abs(Number(diffVal)) : 0;
    const diff = (diffRaw / 10).toFixed(1);

    let direction = 0;
    if (Number.isFinite(Number(actualVal)) && Number.isFinite(Number(predictedVal))) {
        direction = Number(actualVal) - Number(predictedVal);
    } else if (Number.isFinite(Number(diffVal))) {
        direction = Number(diffVal);
    }

    let descriptor;
    if (direction > 0.0001) {
        descriptor = `유량 +${diff}% 상승 감지`;
    } else if (direction < -0.0001) {
        descriptor = `유량 -${diff}% 하락 감지`;
    } else {
        descriptor = `유량 편차 ${diff}% 감지`;
    }
    return `[${label}] ${descriptor}`;
}

function setStatus(message, type = 'info') {
    const statusEl = document.getElementById('status-message');
    if (!statusEl) return;
    statusEl.textContent = message || '';
    statusEl.style.color = type === 'error' ? '#c53030' : '#495057';
}

function getStepName(log) {
    const parsed = parseLogMessage(log.message);
    return parsed?.step_name || parsed?.step || log.step_name || '';
}

function populateStepOptions(names) {
    const select = document.getElementById('stepFilter');
    if (!select) return;
    // 고정 옵션 (데이터 연동 없음)
    select.innerHTML = '<option value="all">pre-NH3</option>';
    select.value = 'all';
    stepFilterValue = 'all';
}

function sortLogs(logs) {
    return [...logs].sort((a, b) => {
        const aTs = new Date(a.timestamp).getTime();
        const bTs = new Date(b.timestamp).getTime();
        if (Number.isNaN(aTs) || Number.isNaN(bTs)) return 0;
        return sortOrder === 'asc' ? aTs - bTs : bTs - aTs;
    });
}

function getSelectedParameters() {
    return Array.from(selectedParams);
}

function renderCategoryButtons() {
    const container = document.getElementById('category-buttons');
    if (!container) return;
    container.innerHTML = '';
    Object.keys(CATEGORY_GROUPS).forEach(cat => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'category-btn';
        btn.textContent = cat;
        if (activeCategories.has(cat)) btn.classList.add('active');
        btn.addEventListener('click', () => {
            if (activeCategories.has(cat)) {
                activeCategories.delete(cat);
            } else {
                activeCategories.add(cat);
            }
            renderCategoryButtons();
            renderParamSections();
        });
        container.appendChild(btn);
    });
}

function renderParamSections() {
    const container = document.getElementById('param-sections');
    if (!container) return;
    container.innerHTML = '';
    const orderedCats = Object.keys(CATEGORY_GROUPS).filter(cat => activeCategories.has(cat));
    orderedCats.forEach(cat => {
        const row = document.createElement('div');
        row.className = 'param-row';

        const title = document.createElement('div');
        title.className = 'param-title';
        title.textContent = cat;
        row.appendChild(title);

        const btnWrap = document.createElement('div');
        btnWrap.className = 'param-buttons';

        const available = CATEGORY_GROUPS[cat].filter(p => predictColumns.includes(p));
        available.forEach(param => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'param-btn';
            btn.textContent = displayParam(param);
            if (selectedParams.has(param)) btn.classList.add('active');
            btn.addEventListener('click', () => {
                if (selectedParams.has(param)) {
                    selectedParams.delete(param);
                    btn.classList.remove('active');
                } else {
                    selectedParams.add(param);
                    btn.classList.add('active');
                }
            });
            btnWrap.appendChild(btn);
        });

        row.appendChild(btnWrap);
        container.appendChild(row);
    });
}

function initializeParameterUI(columns) {
    predictColumns = columns;
    // start with only MFC7_DCS selected if available
    selectedParams = new Set();
    if (predictColumns.includes('MFC7_DCS')) {
        selectedParams.add('MFC7_DCS');
    }
    activeCategories = new Set(Object.keys(CATEGORY_GROUPS));
    renderCategoryButtons();
    renderParamSections();
}

function setDefaultRange() {
    const endInput = document.getElementById('endTime');
    const startInput = document.getElementById('startTime');
    if (!endInput || !startInput) return;
    const now = new Date();
    const end = new Date(now.getTime() - now.getSeconds() * 1000);
    const start = new Date(end.getTime() - 60 * 60 * 1000);
    const toValue = (date) => {
        const pad = (n) => String(n).padStart(2, '0');
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
    };
    endInput.value = toValue(end);
    startInput.value = toValue(start);
}

function toTimestampString(value) {
    if (!value) return null;
    // datetime-local is in local timezone without seconds
    return value.replace('T', ' ') + ':00';
}

function parseTimestamp(value) {
    if (!value) return null;
    const iso = value.replace(' ', 'T');
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return null;
    return date;
}

function parseLogRange(log) {
    if (!log) return null;
    let start = null;
    let end = null;
    try {
        const parsed = JSON.parse(log.message || '{}');
        start = parsed.start || null;
        end = parsed.end || null;
    } catch (e) {
        // ignore parse errors; fall back to timestamp only
    }
    const ts = log.timestamp ? String(log.timestamp).replace(' ', 'T') : null;
    if (!start && ts) start = ts;
    if (!end && ts) end = ts;
    const startDate = start ? new Date(start) : null;
    const endDate = end ? new Date(end) : null;
    if (!startDate || Number.isNaN(startDate.getTime())) return null;
    const endValid = endDate && !Number.isNaN(endDate.getTime()) ? endDate : startDate;
    return { start: startDate, end: endValid };
}

function getLogKey(log) {
    return `${log.parameter}-${log.timestamp}`;
}

function buildHighlightRegions(logs) {
    const regions = [];
    logs.forEach(log => {
        const range = parseLogRange(log);
        if (!range) return;
        const startMs = range.start.getTime();
        const endMs = range.end.getTime();
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return;
        const safeEnd = endMs >= startMs ? endMs : startMs;
        regions.push({ start: startMs, end: safeEnd });
    });
    return regions;
}

function updateHighlightRegionsForParam(param) {
    const logs = logsByParameter.get(param) || [];
    const filtered = logs.filter(log => selectedLogKeys.has(getLogKey(log)));
    const regions = buildHighlightRegions(filtered);
    highlightRegionsByParam.set(param, regions);
}

function refreshChartHighlight(param) {
    const chart = charts.get(param);
    if (!chart) return;
    updateHighlightRegionsForParam(param);
    chart.update();
}

function refreshAllChartHighlights() {
    charts.forEach((_chart, param) => {
        updateHighlightRegionsForParam(param);
    });
    charts.forEach(chart => chart.update());
}

function refreshCardHighlight(param) {
    const card = chartCards.get(param);
    if (!card) return;
    const hasSelected = (logsByParameter.get(param) || []).some(log => selectedLogKeys.has(getLogKey(log)));
    if (hasSelected) {
        card.classList.add('chart-selected');
    } else {
        card.classList.remove('chart-selected');
    }
}

function scrollChartIntoView(param) {
    const card = chartCards.get(param);
    if (!card) return;
    card.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function getTopLogElement(param) {
    let target = null;
    logElements.forEach(el => {
        if (el.dataset.param !== param) return;
        if (!target || el.offsetTop < target.offsetTop) target = el;
    });
    return target;
}

function scrollLogIntoView(param) {
    const target = getTopLogElement(param);
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function setLogElementState(key, active) {
    const el = logElements.get(key);
    if (!el) return;
    if (active) {
        el.classList.add('log-selected');
    } else {
        el.classList.remove('log-selected');
    }
}

function toggleLogSelection(log) {
    const key = getLogKey(log);
    const wasSelected = selectedLogKeys.has(key);
    if (wasSelected) {
        selectedLogKeys.delete(key);
        setLogElementState(key, false);
    } else {
        selectedLogKeys.add(key);
        setLogElementState(key, true);
    }
    refreshChartHighlight(log.parameter);
    refreshCardHighlight(log.parameter);
    if (!wasSelected) {
        scrollChartIntoView(log.parameter);
    }
}

function setParamLogSelection(param, enabled) {
    const logs = logsByParameter.get(param) || [];
    logs.forEach(log => {
        const key = getLogKey(log);
        if (enabled) {
            selectedLogKeys.add(key);
        } else {
            selectedLogKeys.delete(key);
        }
        setLogElementState(key, enabled);
    });
    refreshChartHighlight(param);
    refreshCardHighlight(param);
}

function handleChartClick(param) {
    const logs = logsByParameter.get(param) || [];
    if (!logs.length) return;
    const allSelected = logs.every(log => selectedLogKeys.has(getLogKey(log)));
    const enable = !allSelected;
    setParamLogSelection(param, enable);
    refreshCardHighlight(param);
    if (enable) {
        scrollLogIntoView(param);
    }
}

function createChartCard(parameter) {
    const container = document.getElementById('charts-container');
    const card = document.createElement('div');
    card.className = 'chart-card';
    const safe = safeId(parameter);
    card.innerHTML = `
        <div class="chart-header">
            <div class="chart-title">${displayParam(parameter)}</div>
        </div>
        <canvas id="chart-${safe}"></canvas>
        <div class="chart-message" id="message-${safe}"></div
    `;
    card.addEventListener('click', () => handleChartClick(parameter));
    container.appendChild(card);
    chartCards.set(parameter, card);
    return {
        card,
        canvas: card.querySelector('canvas'),
        messageEl: card.querySelector('.chart-message')
    };
}

function renderChart(parameter, actual, predicted, logs) {
    const safe = safeId(parameter);
    const canvas = document.getElementById(`chart-${safe}`);
    if (!canvas) return;
    if (charts.has(parameter)) {
        charts.get(parameter).destroy();
        charts.delete(parameter);
    }

    const yValues = actual.concat(predicted).map(d => d.y).filter(v => v !== null && v !== undefined);
    if (!yValues.length) return;
    const yMin = Math.min(...yValues);
    const yMax = Math.max(...yValues);

    const chart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            datasets: [
                {
                    label: '예측값',
                    data: predicted.map(d => ({ x: d.x, y: d.y })),
                    borderColor: '#ef5350',
                    borderWidth: 1.2,
                    tension: 0.3,
                    pointRadius: 0,
                },
                {
                    label: '실제값',
                    data: actual.map(d => ({ x: d.x, y: d.y })),
                    borderColor: '#1e88e5',
                    borderWidth: 1.2,
                    tension: 0.3,
                    pointRadius: 0,
                }
            ]
        },
        options: {
            animation: false,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: { usePointStyle: true }
                },
                selectionHighlight: {
                    getRegions: (paramName) => highlightRegionsByParam.get(paramName) || []
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { tooltipFormat: 'yyyy-MM-dd HH:mm:ss' }
                },
                y: {
                    min: yMin,
                    max: yMax,
                    ticks: { autoSkip: true }
                }
            }
        }
    });

    chart._codexParam = parameter;
    updateHighlightRegionsForParam(parameter);
    charts.set(parameter, chart);
}

function destroyCharts() {
    charts.forEach(chart => chart.destroy());
    charts.clear();
    chartCards.clear();
}

function renderLogList(logs) {
    const container = document.getElementById('log-list');
    if (!container) return;
    logElements.clear();
    selectedLogKeys.clear();
    highlightRegionsByParam.clear();
    container.innerHTML = '';
    const ordered = sortLogs(logs);
    if (!ordered.length) {
        const empty = document.createElement('div');
        empty.textContent = '선택한 기간에 해당하는 이상 로그가 없습니다.';
        empty.style.color = '#666';
        container.appendChild(empty);
        return;
    }
    ordered.forEach(log => {
        const key = getLogKey(log);
        const entry = document.createElement('div');
        entry.className = 'timeline-entry';
        entry.dataset.key = key;
        entry.dataset.param = log.parameter;

        const iconEl = document.createElement('div');
        iconEl.className = 'timeline-icon';
        iconEl.textContent = '⚠';

        const bodyEl = document.createElement('div');
        bodyEl.className = 'timeline-body';

        const timeEl = document.createElement('div');
        timeEl.className = 'timeline-time';
        timeEl.textContent = formatFullKorean(log.timestamp);

        const textEl = document.createElement('div');
        textEl.className = 'timeline-text';
        textEl.textContent = buildLogDescription(log);

        bodyEl.appendChild(timeEl);
        bodyEl.appendChild(textEl);

        entry.appendChild(iconEl);
        entry.appendChild(bodyEl);
        entry.addEventListener('click', () => toggleLogSelection(log));
        container.appendChild(entry);
        logElements.set(key, entry);
    });
    refreshAllChartHighlights();
    charts.forEach((_chart, param) => refreshCardHighlight(param));
}

function updateLogPanelHeight() {
    const chartsArea = document.getElementById('charts-area');
    const logArea = document.getElementById('log-area');
    if (!chartsArea || !logArea) return;
    const target = chartsArea.offsetHeight;
    if (target > 0) {
        logArea.style.height = `${target}px`;
        logArea.style.maxHeight = `${target}px`;
    }
}

async function fetchLogs(start, end, params = []) {
    logsByParameter = new Map();
    try {
        const res = await fetch(`/api/history/logs?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`);
        if (!res.ok) throw new Error('로그 조회 실패');
        const data = await res.json();
        const filteredByParam = params.length ? data.filter(item => params.includes(item.parameter)) : data;
        const stepNames = Array.from(new Set(filteredByParam.map(getStepName).filter(Boolean)));
        populateStepOptions(stepNames);
        const filtered = filteredByParam.filter(item => stepFilterValue === 'all' ? true : getStepName(item) === stepFilterValue);
        const ordered = sortLogs(filtered);
        ordered.forEach(item => {
            const key = item.parameter;
            if (!logsByParameter.has(key)) logsByParameter.set(key, []);
            logsByParameter.get(key).push(item);
        });
        renderLogList(ordered);
    } catch (err) {
        renderLogList([]);
        console.error(err);
        setStatus('로그를 불러오지 못했습니다. 입력한 기간과 서버 상태를 확인하세요.', 'error');
    }
}

async function loadHistory() {
    const startValue = toTimestampString(document.getElementById('startTime')?.value);
    const endValue = toTimestampString(document.getElementById('endTime')?.value);
    if (!startValue || !endValue) {
        setStatus('시작 시간과 종료 시간을 모두 입력해주세요.', 'error');
        return;
    }
    const startDate = parseTimestamp(startValue);
    const endDate = parseTimestamp(endValue);
    if (!startDate || !endDate) {
        setStatus('시간 형식이 올바르지 않습니다.', 'error');
        return;
    }
    if (startDate > endDate) {
        setStatus('시작 시간이 종료 시간보다 늦을 수 없습니다.', 'error');
        return;
    }

    const params = getSelectedParameters();
    if (!params.length) {
        setStatus('최소 한 개의 파라미터를 선택해주세요.', 'error');
        return;
    }

    const requestId = ++currentRequestId;
    setStatus('데이터를 불러오는 중입니다...');
    destroyCharts();
    const chartsContainer = document.getElementById('charts-container');
    chartsContainer.innerHTML = '';

    await fetchLogs(startValue, endValue, params);
    if (requestId !== currentRequestId) return;

    for (const param of params) {
        const { messageEl } = createChartCard(param);
        const chartLogs = logsByParameter.get(param) || [];
        try {
            const res = await fetch(`/api/trace_pred_chart?param=${encodeURIComponent(param)}&start=${encodeURIComponent(startValue)}&end=${encodeURIComponent(endValue)}`);
            if (!res.ok) throw new Error('데이터 조회 실패');
            const json = await res.json();
            if (requestId !== currentRequestId) return;

            const actual = json.actual || [];
            const predicted = json.predicted || [];
            if (!actual.length && !predicted.length) {
                if (messageEl) {
                    messageEl.textContent = '표시할 데이터가 없습니다.';
                }
                continue;
            }
            if (messageEl) messageEl.textContent = '';

            renderChart(param, actual, predicted, chartLogs);
        } catch (err) {
            console.error(err);
            if (messageEl) {
                messageEl.textContent = '데이터를 불러오는데 실패했습니다.';
            }
        }
    }

    setStatus('조회가 완료되었습니다.');
    updateLogPanelHeight();
}

async function initPage() {
    setDefaultRange();
    try {
        const res = await fetch('/api/model_columns');
        if (!res.ok) throw new Error('모델 컬럼 조회 실패');
        predictColumns = await res.json();
        initializeParameterUI(predictColumns);
    } catch (err) {
        console.error(err);
        setStatus('예측 컬럼 정보를 불러오지 못했습니다.', 'error');
    }

    document.getElementById('selectAll')?.addEventListener('click', () => {
        Object.keys(CATEGORY_GROUPS).forEach(cat => {
            CATEGORY_GROUPS[cat].forEach(p => {
                if (predictColumns.includes(p)) selectedParams.add(p);
            });
        });
        renderParamSections();
    });
    document.getElementById('clearAll')?.addEventListener('click', () => {
        selectedParams.clear();
        renderParamSections();
    });
    document.getElementById('searchBtn')?.addEventListener('click', () => {
        loadHistory();
    });
    document.getElementById('stepFilter')?.addEventListener('change', (e) => {
        stepFilterValue = e.target.value || 'all';
        loadHistory();
    });
    document.getElementById('sortOrder')?.addEventListener('change', (e) => {
        sortOrder = e.target.value === 'asc' ? 'asc' : 'desc';
        loadHistory();
    });

    window.addEventListener('resize', updateLogPanelHeight);

    // 초기 로드
    loadHistory();
}

window.addEventListener('DOMContentLoaded', initPage);
