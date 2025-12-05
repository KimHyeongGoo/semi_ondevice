const processCharts = {};
const recentCharts = {};
const chartBoxesMap = {};
const logEntryElements = new Map();
let processStart = null;
let processEnd = null;
// logs structure: { param: [ {start, end, diff, actual_value, predicted_value, peak_time, step_id, step_name} ] }
let logs = {};
const loggedIds = new Set();
let lastServerLogEnd = 0;
let lastSeenLogEnd = 0;
let currentHighlight = null;
let warningModalOpen = false;
let modalChart = null; // ECharts instance for modal
let modalChartFallback = null; // Chart.js fallback when ECharts unavailable
let modalInfo = null;
const warningToggleKey = 'anomalyWarningEnabled';
let warningEnabled = true;
let settingsCache = {};
const storedWarning = typeof localStorage !== 'undefined' ? localStorage.getItem(warningToggleKey) : null;
if (storedWarning !== null) warningEnabled = storedWarning === 'true';
let deviceStatus = { status: 'RUN', lastTick: null };

let latestStepTimestamp = 0;
let lastStepUpdate = 0;
let lastStepFallbackFetch = 0;
let warningElements = {};
let reportElements = {};
let currentReportEntry = null;
let reportChart = null;
const mfcParams = new Set(['MFC7_DCS', 'MFC8_NH3', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4']);
let threeViewer = null;
const htmlCache = new Map();

function updateCurrentStepDisplay(stepId, stepName) {
    const idEl = document.getElementById('current-step-id');
    const nameEl = document.getElementById('current-step-name');
    if (idEl) {
        idEl.textContent = stepId !== null && stepId !== undefined ? stepId : '-';
    }
    if (nameEl) {
        nameEl.textContent = stepName ? stepName : '-';
    }
    lastStepUpdate = Date.now();
}

function considerActualStepInfo(actual) {
    for (let i = actual.length - 1; i >= 0; i--) {
        const entry = actual[i];
        if (!entry) continue;
        const hasInfo = (entry.step_id !== null && entry.step_id !== undefined) || (entry.step_name && entry.step_name !== '');
        if (!hasInfo) continue;
        const timeValue = new Date(entry.x).getTime();
        if (Number.isNaN(timeValue)) continue;
        if (timeValue > latestStepTimestamp) {
            latestStepTimestamp = timeValue;
            updateCurrentStepDisplay(entry.step_id ?? null, entry.step_name ?? null);
        }
        break;
    }
}

async function fetchCurrentStepFallback(force = false) {
    const now = Date.now();
    if (!force && now - lastStepUpdate < 3000) {
        return;
    }
    if (!force && now - lastStepFallbackFetch < 3000) {
        return;
    }
    lastStepFallbackFetch = now;
    try {
        const res = await fetch('/api/current_step');
        if (!res.ok) throw new Error('Failed to fetch current step');
        const data = await res.json();
        updateCurrentStepDisplay(data.step_id ?? null, data.step_name ?? null);
    } catch (e) {
        updateCurrentStepDisplay(null, null);
    }
}

async function loadSettings() {
    try {
        const res = await fetch('/api/settings');
        if (!res.ok) return;
        const s = await res.json();
        settingsCache = s || {};
        if (typeof s.warning_enabled === 'boolean') {
            warningEnabled = s.warning_enabled;
        }
    } catch (e) {
        console.error('failed to load settings', e);
    }
}

async function saveWarningSetting() {
    const body = { ...settingsCache, warning_enabled: warningEnabled };
    try {
        await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        settingsCache = body;
    } catch (e) {
        console.error('failed to save warning toggle', e);
    }
}

const visibilityKey = 'chartVisibilityMode';
let visibilityMode = localStorage.getItem(visibilityKey);
if (!visibilityMode) {
    visibilityMode = 'both';
    localStorage.setItem(visibilityKey, visibilityMode);
}
const visibilityLabels = {
    both: '실제값 + 예측값',
    actual: '실제값만',
    predicted: '예측값만'
};

function setDatasetVisibility(chart, mode) {
    const pred = chart.getDatasetMeta(0);
    const act = chart.getDatasetMeta(1);
    if (mode === 'actual') {
        pred.hidden = true;
        act.hidden = false;
    } else if (mode === 'predicted') {
        pred.hidden = false;
        act.hidden = true;
    } else {
        pred.hidden = false;
        act.hidden = false;
    }
}

function applyVisibilityAll() {
    Object.values(processCharts).forEach(c => { setDatasetVisibility(c, visibilityMode); c.update(); });
    Object.values(recentCharts).forEach(c => { setDatasetVisibility(c, visibilityMode); c.update(); });
}

const highlightPlugin = {
    id: 'highlightRegion',
    beforeDatasetsDraw(chart, args, opts) {
        const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
        ctx.save();
        ctx.fillStyle = 'rgba(173,255,47,0.4)';
        (opts.regions || []).forEach(r => {
            const xStart = x.getPixelForValue(r.start);
            const xEnd = x.getPixelForValue(r.end);
            ctx.fillRect(xStart, top, xEnd - xStart, bottom - top);
        });
        ctx.restore();
    }
};
Chart.register(highlightPlugin);

function formatTime(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return '-';
    return d.toLocaleTimeString('ko-KR', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function parseTimestamp(ts) {
    if (ts instanceof Date) return ts;
    if (typeof ts === 'number') return new Date(ts);
    if (typeof ts === 'string') {
        // 공백 구분 포맷을 ISO로 치환
        return new Date(ts.replace(' ', 'T'));
    }
    return new Date(ts);
}

function safeId(name) {
    return name.replace(/[ .-]/g, '_');
}

function setDeviceState(status, lastTick) {
    const toggle = document.getElementById('device-toggle');
    const runEl = document.getElementById('device-state-run');
    const downEl = document.getElementById('device-state-down');
    deviceStatus = { status, lastTick };
    if (!toggle || !runEl || !downEl) return;
    const isRun = status === 'RUN';
    toggle.classList.toggle('down-active', !isRun);
    runEl.classList.toggle('active', isRun);
    downEl.classList.toggle('active', !isRun);
}

async function pollGeneratorStatus() {
    try {
        const res = await fetch('/api/generator_status');
        if (!res.ok) throw new Error('bad response');
        const json = await res.json();
        const status = json?.status === 'RUN' ? 'RUN' : 'DOWN';
        setDeviceState(status, json?.last_tick ?? null);
    } catch (e) {
        setDeviceState('DOWN', null);
    }
}

function createCharts() {
    const xAxis = {
        type: 'time',
        time: { unit: 'second', tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm:ss' } },
        ticks: {
            source: 'data',
            autoSkip: true,
            maxRotation: 0,
            autoSkipPadding: 12,
            callback: function (value) {
                const label = this.getLabelForValue(value);
                return formatTime(label);
            }
        },
        adapters: { date: {} }
    };
    columns.forEach(col => {
        const id = safeId(col);
        const pctx = document.getElementById(`proc-${id}`).getContext('2d');
        const rctx = document.getElementById(`recent-${id}`).getContext('2d');
        const rowEl = document.querySelector(`.chart-row[data-param="${col}"]`);
        if (rowEl) {
            const boxes = Array.from(rowEl.querySelectorAll('.chart-box'));
            chartBoxesMap[col] = boxes;
            boxes.forEach(box => {
                box.addEventListener('click', () => handleChartClick(col));
            });
        }
        processCharts[col] = new Chart(pctx, {
            type: 'line',
            data: {
                datasets: [
                    { label: '예측값', borderColor: 'red', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] },
                    { label: '실제값', borderColor: 'blue', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] }
                ]
            },
            options: {
                animation: false,
                responsive: true,
                maintainAspectRatio: false,
                plugins: { highlightRegion: { regions: [] } },
                scales: { x: xAxis, y: {} }
            }
        });
        recentCharts[col] = new Chart(rctx, {
            type: 'line',
            data: {
                datasets: [
                    { label: '예측값', borderColor: 'red', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] },
                    { label: '실제값', borderColor: 'blue', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] }
                ]
            },
            options: {
                animation: false,
                responsive: true,
                maintainAspectRatio: false,
                plugins: { highlightRegion: { regions: [] } },
                scales: { x: xAxis, y: {} }
            }
        });
        setDatasetVisibility(processCharts[col], visibilityMode);
        setDatasetVisibility(recentCharts[col], visibilityMode);
    });
    applyVisibilityAll();
    applyHighlightState();
}

function getEntryKey(param, entry) {
    return `${param}-${entry.start}-${entry.end}`;
}

function formatTimelineTime(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) {
        return '-';
    }
    return d.toLocaleTimeString('ko-KR', { hour12: false });
}

function buildLogText(param, entry) {
    const diff = entry.diff != null ? Math.abs(entry.diff).toFixed(0) : '0';
    let direction = 0;
    if (entry.actual_value != null && entry.predicted_value != null) {
        direction = entry.actual_value - entry.predicted_value;
    }
    let descriptor;
    if (direction > 0.0001) {
        descriptor = `유량 +${diff}% 상승 감지`;
    } else if (direction < -0.0001) {
        descriptor = `유량 -${diff}% 하락 감지`;
    } else {
        descriptor = `유량 편차 ${diff}% 감지`;
    }
    return `${param} ${descriptor}`;
}

function tryParseMessage(msg) {
    if (!msg) return {};
    try {
        return JSON.parse(msg);
    } catch (e) {
        return {};
    }
}

function normalizeLogEntry(item) {
    const parsed = tryParseMessage(item?.message);
    const param = parsed.parameter || item.parameter;
    if (!param) return null;
    const startStr = parsed.start || item.start_time || item.timestamp;
    const endStr = parsed.end || item.end_time || item.timestamp;
    const startMs = startStr ? new Date(startStr).getTime() : null;
    const endMs = endStr ? new Date(endStr).getTime() : null;
    return {
        param,
        entry: {
            start: startMs,
            end: endMs,
            diff: parsed.diff_percent ?? parsed.diff ?? item.avg_diff_percent ?? null,
            duration_seconds: parsed.duration_seconds ?? item.duration_seconds ?? null,
            step_id: parsed.step_id || [],
            step_name: parsed.step_name || [],
            actual_value: parsed.actual_value ?? null,
            predicted_value: parsed.predicted_value ?? null,
            peak_time: parsed.peak_time ? new Date(parsed.peak_time).getTime() : null,
            violation_type: parsed.violation_type ?? item.violation_type ?? null,
            violation_type: parsed.violation_type ?? item.violation_type ?? null,
        },
    };
}

function setServerLogs(list) {
    logs = {};
    const prevMax = lastServerLogEnd;
    lastServerLogEnd = 0;
    const newEntries = [];
    list.forEach(item => {
        const norm = normalizeLogEntry(item);
        if (!norm) return;
        if (!logs[norm.param]) logs[norm.param] = [];
        logs[norm.param].push(norm.entry);
        if (norm.entry.end) {
            lastServerLogEnd = Math.max(lastServerLogEnd, norm.entry.end);
            if (norm.entry.end > prevMax) newEntries.push({ param: norm.param, entry: norm.entry });
        }
    });
    Object.values(logs).forEach(arr => arr.sort((a, b) => b.start - a.start));
    updateLog();
    applyHighlightState();

    if (warningEnabled && !warningModalOpen && newEntries.length) {
        const first = newEntries[0];
        openWarningModal(first.param, buildLogText(first.param, first.entry));
    }
    if (lastServerLogEnd > lastSeenLogEnd) {
        lastSeenLogEnd = lastServerLogEnd;
    }
}

function getHighlightRegions(param, startMs, endMs) {
    const entries = logs[param] || [];
    if (startMs == null || endMs == null) return [];
    return entries.map(e => ({
        start: Math.max(e.start ?? -Infinity, startMs),
        end: Math.min(e.end ?? Infinity, endMs),
    })).filter(r => Number.isFinite(r.start) && Number.isFinite(r.end) && r.end > r.start);
}

function applyHighlightState() {
    Object.values(chartBoxesMap).forEach(arr => {
        if (!arr) return;
        arr.forEach(box => box.classList.remove('highlight'));
    });
    logEntryElements.forEach(el => el.classList.remove('highlight'));
    if (!currentHighlight) return;
    const { param, key } = currentHighlight;
    if (chartBoxesMap[param]) {
        chartBoxesMap[param].forEach(box => box.classList.add('highlight'));
    }
    logEntryElements.forEach((el, entryKey) => {
        const matchesKey = key && entryKey === key;
        const matchesParam = key === null && el.dataset.param === param;
        if (matchesKey || matchesParam) {
            el.classList.add('highlight');
        }
    });
}

function setHighlight(param, key, options = {}) {
    if (currentHighlight && currentHighlight.param === param && currentHighlight.key === key) {
        currentHighlight = null;
    } else {
        currentHighlight = { param, key };
    }
    applyHighlightState();
    const active = currentHighlight && currentHighlight.param;
    const shouldScrollLog = options.scrollLog || options.scroll;
    if (active && options.scrollChart) {
        const boxes = chartBoxesMap[currentHighlight.param];
        if (boxes && boxes.length) {
            boxes[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
    if (active && shouldScrollLog) {
        let target = null;
        if (currentHighlight.key && logEntryElements.has(currentHighlight.key)) {
            target = logEntryElements.get(currentHighlight.key);
        }
        if (!target) {
            logEntryElements.forEach(el => {
                if (el.dataset.param !== currentHighlight.param) return;
                if (!target || el.offsetTop < target.offsetTop) target = el;
            });
        }
        if (target) {
            target.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

function handleLogClick(param, key) {
    setHighlight(param, key, { scrollChart: true });
}

function handleChartClick(param) {
    const entries = logs[param];
    if (!entries || !entries.length) {
        setHighlight(param, null);
        return;
    }
    // 전체 로그를 강조하기 위해 key는 null로 설정
    const isActive = currentHighlight && currentHighlight.param === param && currentHighlight.key === null;
    setHighlight(param, null, { scrollLog: !isActive });
}

function updateLog() {
    const logDiv = document.getElementById('log-content');
    if (!logDiv) return;
    logEntryElements.clear();
    logDiv.innerHTML = '';
    const allEntries = [];
    Object.entries(logs).forEach(([param, arr]) => {
        arr.forEach(entry => {
            allEntries.push({ param, ...entry });
        });
    });
    allEntries.sort((a, b) => b.end - a.end);
    allEntries.forEach(entry => {
        const key = getEntryKey(entry.param, entry);
        const wrapper = document.createElement('div');
        wrapper.className = 'timeline-entry';
        wrapper.dataset.param = entry.param;
        wrapper.dataset.key = key;
        const timeEl = document.createElement('div');
        timeEl.className = 'timeline-time';
        timeEl.textContent = formatTimelineTime(entry.end);
        const textEl = document.createElement('div');
        textEl.className = 'timeline-text';
        textEl.textContent = buildLogText(entry.param, entry);
        const iconEl = document.createElement('div');
        iconEl.className = 'timeline-icon';
        iconEl.textContent = '⚠';
        const reportBtn = document.createElement('button');
        reportBtn.className = 'report-btn';
        reportBtn.textContent = 'Report';
        reportBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            openReportModal(entry);
        });
        const bodyEl = document.createElement('div');
        bodyEl.className = 'timeline-body';
        bodyEl.appendChild(timeEl);
        bodyEl.appendChild(textEl);
        wrapper.appendChild(iconEl);
        wrapper.appendChild(bodyEl);
        wrapper.appendChild(reportBtn);
        wrapper.addEventListener('click', () => handleLogClick(entry.param, key));
        logDiv.appendChild(wrapper);
        logEntryElements.set(key, wrapper);
    });
    if (currentHighlight && currentHighlight.key && !logEntryElements.has(currentHighlight.key)) {
        currentHighlight = null;
    }
    applyHighlightState();
}

function updateLogPanelHeight() {
    const chartsEl = document.getElementById('charts-container');
    const logPanel = document.getElementById('log-panel');
    if (chartsEl && logPanel) {
        logPanel.style.maxHeight = chartsEl.offsetHeight + 'px';
    }
}

function trimLogEntries(limit = 20) {
    const all = [];
    Object.entries(logs).forEach(([param, arr]) => {
        arr.forEach(l => all.push({ param, ...l }));
    });
    all.sort((a, b) => b.start - a.start);
    const trimmed = all.slice(0, limit);
    logs = {};
    loggedIds.clear();
    trimmed.forEach(l => {
        if (!logs[l.param]) logs[l.param] = [];
        logs[l.param].push({
            start: l.start,
            end: l.end,
            diff: l.diff,
            step_id: l.step_id,
            step_name: l.step_name,
            actual_value: l.actual_value,
            predicted_value: l.predicted_value,
            peak_time: l.peak_time
        });
        loggedIds.add(`${l.param}-${l.start}-${l.end}`);
    });
    Object.values(logs).forEach(arr => arr.sort((a, b) => b.start - a.start));
}


function updateCharts(col, data) {
    const actual = data.actual.map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    const predicted = data.predicted.map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    considerActualStepInfo(actual);
    const pChart = processCharts[col];
    pChart.data.datasets[0].data = predicted;
    pChart.data.datasets[1].data = actual;
    const allTimestamps = actual.concat(predicted).map(d => new Date(d.x).getTime());
    let xMin = null;
    let xMax = null;
    if (allTimestamps.length) {
        xMin = Math.min(...allTimestamps);
        xMax = Math.max(...allTimestamps);
        pChart.options.scales.x.min = xMin;
        pChart.options.scales.x.max = xMax;
    }
    const regions = xMin !== null && xMax !== null ? getHighlightRegions(col, xMin, xMax) : [];
    pChart.options.plugins.highlightRegion.regions = regions;
    const allVals = actual.concat(predicted).map(d => d.y);
    if (allVals.length) {
        const max = Math.max(...allVals);
        const min = Math.min(...allVals);
        let pad = 3;
        if (col.startsWith('Temp_Act')) pad = 100;
        else if (col.includes('VG11')) pad = 1;
        else if (col.includes('POS')) pad = 10;
        pChart.options.scales.y.max = max + pad;
        pChart.options.scales.y.min = min - pad;
    }
    setDatasetVisibility(pChart, visibilityMode);
    pChart.update();

    const lastActual = actual.length ? new Date(actual[actual.length - 1].x).getTime() : 0;
    const lastPred = predicted.length ? new Date(predicted[predicted.length - 1].x).getTime() : 0;
    let recentEnd = Math.max(lastActual, lastPred);
    if (!recentEnd) recentEnd = Date.now();
    const recentStart = recentEnd - 60000;
    const aRecent = actual.filter(d => new Date(d.x).getTime() >= recentStart);
    const pRecent = predicted.filter(d => new Date(d.x).getTime() >= recentStart);
    const rChart = recentCharts[col];
    rChart.data.datasets[0].data = pRecent;
    rChart.data.datasets[1].data = aRecent;
    const reg2 = getHighlightRegions(col, recentStart, recentEnd);
    rChart.options.plugins.highlightRegion.regions = reg2;
    const recentTs = aRecent.concat(pRecent).map(d => new Date(d.x).getTime());
    if (recentTs.length) {
        rChart.options.scales.x.min = Math.min(...recentTs);
        rChart.options.scales.x.max = Math.max(...recentTs);
    }
    const recentVals = aRecent.concat(pRecent).map(d => d.y);
    if (recentVals.length) {
        const max = Math.max(...recentVals);
        const min = Math.min(...recentVals);
        let pad = 3;
        if (col.startsWith('Temp_Act')) pad = 100;
        else if (col.includes('VG11')) pad = 1;
        rChart.options.scales.y.max = max + pad;
        rChart.options.scales.y.min = min - pad;
    }
    setDatasetVisibility(rChart, visibilityMode);
    rChart.update();

}

function cloneDatasets(chart) {
    return chart.data.datasets.map(ds => ({
        ...ds,
        data: ds.data.map(p => ({ ...p }))
    }));
}

function openChartModal(param, kind) {
    const modal = document.getElementById('chart-modal');
    const title = document.getElementById('modal-title');
    const container = document.getElementById('modal-echart');
    const fallbackCanvas = document.getElementById('modal-canvas');
    if (!modal || !title || !container || !fallbackCanvas) return;
    const source = kind === 'recent' ? recentCharts[param] : processCharts[param];
    if (!source) return;
    modal.style.display = 'flex'; // 먼저 열어 컨테이너 크기 확보
    const datasets = cloneDatasets(source);
    const regions = source.options?.plugins?.highlightRegion?.regions || [];
    const canUseEcharts = typeof echarts !== 'undefined';

    // reset visibility
    container.style.display = canUseEcharts ? 'block' : 'none';
    fallbackCanvas.style.display = canUseEcharts ? 'none' : 'block';

    if (canUseEcharts) {
        if (!modalChart) {
            modalChart = echarts.init(container);
        } else {
            modalChart.clear();
        }
        const series = datasets.map((ds, idx) => {
            const data = (ds.data || []).map(p => [p.x, p.y]);
            const serie = {
                name: ds.label || `series ${idx + 1}`,
                type: 'line',
                showSymbol: false,
                smooth: true,
                data,
                lineStyle: { width: ds.borderWidth || 2, color: ds.borderColor || undefined }
            };
            if (idx === 0 && regions.length) {
                serie.markArea = {
                    itemStyle: { color: 'rgba(255,0,0,0.08)' },
                    data: regions.map(r => [{ xAxis: r.start }, { xAxis: r.end }])
                };
            }
            return serie;
        });
        const option = {
            tooltip: { trigger: 'axis' },
            legend: { top: 0 },
            toolbox: {
                feature: {
                    saveAsImage: {},
                    dataZoom: { yAxisIndex: 'none' },
                    restore: {}
                }
            },
            grid: { left: 50, right: 20, top: 40, bottom: 50 },
            xAxis: { type: 'time' },
            yAxis: { type: 'value', scale: true },
            dataZoom: [
                { type: 'inside', xAxisIndex: 0 },
                { type: 'slider', xAxisIndex: 0 }
            ],
            series
        };
        modalChart.setOption(option, true);
        setTimeout(() => modalChart?.resize(), 0);
    } else {
        const ctx = fallbackCanvas.getContext('2d');
        if (modalChartFallback) {
            modalChartFallback.destroy();
            modalChartFallback = null;
        }
        modalChartFallback = new Chart(ctx, {
            type: 'line',
            data: { datasets },
            options: {
                animation: false,
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: true },
                    highlightRegion: { regions }
                },
                scales: { x: { type: 'time' }, y: { type: 'linear' } }
            }
        });
    }
    modalInfo = { param, kind };
    title.textContent = `${param} (${kind === 'recent' ? '최근 1분' : '최근 5분'})`;
    setTimeout(() => {
        if (modalChart) modalChart.resize();
        if (modalChartFallback) modalChartFallback.resize();
    }, 0);
}

function closeChartModal() {
    const modal = document.getElementById('chart-modal');
    if (modal) modal.style.display = 'none';
    if (modalChart) {
        modalChart.clear();
        modalChart = null;
    }
    if (modalChartFallback) {
        modalChartFallback.destroy();
        modalChartFallback = null;
    }
    modalInfo = null;
}

function openWarningModal(param, text) {
    if (!warningEnabled) return;
    if (!warningElements.warningModal || !warningElements.warningParam) return;
    warningElements.warningParam.textContent = text || `${param} 이상 감지`;
    warningElements.warningModal.classList.add('show');
    warningModalOpen = true;
}

function closeWarningModal() {
    if (warningElements.warningModal) warningElements.warningModal.classList.remove('show');
    warningModalOpen = false;
}

function openConfirmModal() {
    closeWarningModal();
    if (warningElements.confirmModal) warningElements.confirmModal.classList.add('show');
}

function closeConfirmModal() {
    if (warningElements.confirmModal) warningElements.confirmModal.classList.remove('show');
}

function buildSeverity(diff) {
    if (!Number.isFinite(diff)) return 'Level 1';
    if (diff >= 80) return 'Level 5';
    if (diff >= 60) return 'Level 4';
    if (diff >= 40) return 'Level 3';
    if (diff >= 20) return 'Level 2';
    return 'Level 1';
}

function destroyReportChart() {
    if (reportChart) {
        reportChart.destroy();
        reportChart = null;
    }
}

async function loadHtml(path, cacheKey) {
    const key = cacheKey || path;
    if (htmlCache.has(key)) return htmlCache.get(key);
    const res = await fetch(path);
    if (!res.ok) throw new Error('failed to fetch html');
    let text = await res.text();
    text = text.replace(/\.\.\/img\//g, '/static/img/'); // 이미지 경로 보정
    htmlCache.set(key, text);
    return text;
}

function renderHtml(htmlContainer, htmlText) {
    if (!htmlContainer) return;
    htmlContainer.innerHTML = htmlText;
}

function disposeThreeViewer() {
    if (!threeViewer) return;
    cancelAnimationFrame(threeViewer.rafId);
    if (threeViewer.cleanup) threeViewer.cleanup();
    if (threeViewer.renderer) threeViewer.renderer.dispose();
    threeViewer = null;
}

async function loadStlGeometry(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error('failed to fetch stl');
    const buffer = await res.arrayBuffer();
    const dv = new DataView(buffer);
    const faceCount = buffer.byteLength >= 84 ? dv.getUint32(80, true) : 0;
    const expectedLength = 84 + faceCount * 50;
    const headText = new TextDecoder().decode(buffer.slice(0, 80));
    const looksAsciiHeader = headText.trim().startsWith('solid');
    const isLikelyBinary = expectedLength === buffer.byteLength;

    // Binary STL
    if (isLikelyBinary) {
        const faceLimit = 1500000; // 최대 150만 면까지 허용
        if (faceCount > faceLimit) {
            throw new Error(`STL faces too large (${faceCount})`);
        }
        const positions = new Float32Array(faceCount * 9);
        const normals = new Float32Array(faceCount * 9);
        let offset = 84;
        for (let i = 0; i < faceCount; i++) {
            const nx = dv.getFloat32(offset, true);
            const ny = dv.getFloat32(offset + 4, true);
            const nz = dv.getFloat32(offset + 8, true);
            offset += 12;
            for (let v = 0; v < 3; v++) {
                const vx = dv.getFloat32(offset, true);
                const vy = dv.getFloat32(offset + 4, true);
                const vz = dv.getFloat32(offset + 8, true);
                const idx = i * 9 + v * 3;
                positions[idx] = vx; positions[idx + 1] = vy; positions[idx + 2] = vz;
                normals[idx] = nx; normals[idx + 1] = ny; normals[idx + 2] = nz;
                offset += 12;
            }
            offset += 2; // attr byte count
        }
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        geometry.setAttribute('normal', new THREE.BufferAttribute(normals, 3));
        geometry.computeBoundingSphere();
        return geometry;
    }

    // ASCII STL fallback
    if (looksAsciiHeader) {
        const text = new TextDecoder().decode(buffer);
        const vertexPattern = /vertex\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)/g;
        const vertices = [];
        let m;
        while ((m = vertexPattern.exec(text)) !== null) {
            vertices.push(parseFloat(m[1]), parseFloat(m[2]), parseFloat(m[3]));
        }
        if (vertices.length / 3 > 5_000_000) {
            throw new Error('STL vertices too large');
        }
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
        geometry.computeVertexNormals();
        return geometry;
    }
    throw new Error('Unsupported STL format');
}

function attachOrbitControls(canvas, camera, target) {
    let isRotating = false;
    let lastX = 0, lastY = 0;
    let radius = camera.position.length();
    const spherical = new THREE.Spherical().setFromVector3(camera.position.clone().sub(target));
    const onPointerDown = (e) => {
        if (e.button !== 0) return;
        isRotating = true;
        lastX = e.clientX; lastY = e.clientY;
    };
    const onPointerMove = (e) => {
        if (!isRotating) return;
        const dx = e.clientX - lastX;
        const dy = e.clientY - lastY;
        lastX = e.clientX; lastY = e.clientY;
        const ROT_SPEED = 0.005;
        spherical.theta -= dx * ROT_SPEED;
        spherical.phi -= dy * ROT_SPEED;
        const EPS = 0.0001;
        spherical.phi = Math.max(EPS, Math.min(Math.PI - EPS, spherical.phi));
        const vec = new THREE.Vector3().setFromSpherical(spherical).add(target);
        camera.position.copy(vec);
        camera.lookAt(target);
    };
    const onPointerUp = () => { isRotating = false; };
    const onWheel = (e) => {
        e.preventDefault();
        e.stopPropagation();
        const delta = e.deltaY * 0.001;
        radius *= (1 + delta);
        radius = Math.max(5, radius);
        spherical.radius = radius;
        const vec = new THREE.Vector3().setFromSpherical(spherical).add(target);
        camera.position.copy(vec);
        camera.lookAt(target);
    };
    canvas.addEventListener('pointerdown', onPointerDown);
    window.addEventListener('pointermove', onPointerMove);
    window.addEventListener('pointerup', onPointerUp);
    canvas.addEventListener('wheel', onWheel, { passive: false });
    return () => {
        canvas.removeEventListener('pointerdown', onPointerDown);
        window.removeEventListener('pointermove', onPointerMove);
        window.removeEventListener('pointerup', onPointerUp);
        canvas.removeEventListener('wheel', onWheel);
    };
}

async function createThreeViewer(container, modelUrl) {
    if (!window.THREE || !container) {
        container.textContent = '3D 라이브러리를 불러오지 못했습니다.';
        return;
    }
    disposeThreeViewer();
    const width = container.clientWidth || 600;
    const height = 420;
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(width, height);
    renderer.setClearColor(0xf8fafc, 1);
    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 5000);
    camera.position.set(0, 0, 200);

    scene.add(new THREE.AmbientLight(0xffffff, 0.6));
    const dir = new THREE.DirectionalLight(0xffffff, 0.8);
    dir.position.set(60, 80, 120);
    scene.add(dir);

    let cleanup = null;
    try {
        const geometry = await loadStlGeometry(modelUrl);
        geometry.center();
        // 중립적인 그레이 톤으로 렌더링
        // 더 밝은 그레이 톤
        const material = new THREE.MeshPhongMaterial({ color: 0xe6e6e6, specular: 0x666666, shininess: 18 });
        const mesh = new THREE.Mesh(geometry, material);
        scene.add(mesh);
        const box = new THREE.Box3().setFromObject(mesh);
        const size = box.getSize(new THREE.Vector3()).length() || 100;
        const center = box.getCenter(new THREE.Vector3());
        camera.position.copy(center).add(new THREE.Vector3(size * 0.6, size * 0.6, size * 0.6));
        camera.near = size / 200;
        camera.far = size * 20;
        camera.updateProjectionMatrix();
        camera.lookAt(center);
        cleanup = attachOrbitControls(renderer.domElement, camera, center.clone());
    } catch (err) {
        console.error('STL load failed', err);
        const tooLarge = String(err?.message || '').includes('too large');
        container.innerHTML = tooLarge ? '모델이 너무 커서 표시할 수 없습니다. 용량을 줄인 STL을 제공해 주세요.' : '3D 모델을 불러오지 못했습니다.';
        return;
    }

    function animate() {
        renderer.render(scene, camera);
        threeViewer.rafId = requestAnimationFrame(animate);
    }
    threeViewer = { renderer, scene, camera, rafId: requestAnimationFrame(animate), cleanup };
}

function buildCauseHeadline(entry) {
    const diff = entry.diff != null ? Math.abs(entry.diff).toFixed(0) : '0';
    const direction = (entry.actual_value ?? 0) - (entry.predicted_value ?? 0);
    if (direction > 0.0001) return `유량 +${diff}% 변화 상승 감지`;
    if (direction < -0.0001) return `유량 -${diff}% 변화 하강 감지`;
    return `유량 편차 ${diff}% 감지`;
}

function replaceCanvasWithMessage(canvas, text) {
    if (!canvas) return;
    const msg = document.createElement('div');
    msg.className = 'report-cause-lines';
    msg.textContent = text;
    canvas.replaceWith(msg);
}

async function drawReportChart(entry, canvas) {
    if (!canvas) return;
    destroyReportChart();
    const ctx = canvas.getContext('2d');
    canvas.style.height = '300px';
    canvas.height = 300;
    const centerRaw = entry.peak_time ?? entry.end ?? entry.start ?? Date.now();
    const center = typeof centerRaw === 'string' ? new Date(centerRaw).getTime() : centerRaw;
    const startIso = new Date(center - 5000).toISOString();
    const endIso = new Date(center + 5000).toISOString();
    let payload;
    try {
        const res = await fetch(`/api/event_chart?param=${encodeURIComponent(entry.param)}&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(endIso)}`);
        if (!res.ok) throw new Error('bad response');
        payload = await res.json();
    } catch (err) {
        console.error('failed to load report chart', err);
        replaceCanvasWithMessage(canvas, '차트 데이터를 불러오지 못했습니다.');
        return;
    }
    const actual = (payload?.actual || []).map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    const predicted = (payload?.predicted || []).map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    const regions = [];
    if (entry.start && entry.end) {
        regions.push({ start: entry.start, end: entry.end });
    }
    if (!actual.length && !predicted.length) {
        replaceCanvasWithMessage(canvas, '해당 구간 데이터가 없습니다.');
        return;
    }
    const values = actual.concat(predicted).map(d => d.y);
    const yMax = values.length ? Math.max(...values) : undefined;
    const yMin = values.length ? Math.min(...values) : undefined;
    reportChart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                { label: '예측값', borderColor: 'red', tension: 0.25, borderWidth: 3, pointRadius: 0, data: predicted },
                { label: '실제값', borderColor: 'blue', tension: 0.25, borderWidth: 3, pointRadius: 0, data: actual }
            ]
        },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true },
                highlightRegion: { regions }
            },
            scales: {
                x: {
                    type: 'time',
                    min: parseTimestamp(startIso),
                    max: parseTimestamp(endIso),
                    time: { unit: 'second', tooltipFormat: 'HH:mm:ss' },
                    ticks: { autoSkip: true, maxRotation: 0 }
                },
                y: {
                    type: 'linear',
                    suggestedMax: yMax !== undefined ? yMax + 0.5 : undefined,
                    suggestedMin: yMin !== undefined ? yMin - 0.5 : undefined
                }
            }
        }
    });
}

async function renderMfcCauseTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    const vt = Number(entry?.violation_type);
    const headline = buildCauseHeadline(entry);
    const causeTexts = {
        1: 'MFC Zero Point Drift 발생으로 인해 기준 유량이 정확히 설정되지 않아 실제 유량 편차 및 공정 불안정이 확인되었습니다.',
        2: 'Baratron 게이지 이상으로 인한 공정 불안정(압력 오차/경보/읽힘 불가)이 확인되었습니다.',
        3: '보트 엘리베이터 회전부의 Magnetic Seal(자기 유체 씰) 성능 저하로 인해 미세 누설 또는 챔버 내 압력 변동이 발생, 이로 인해 MFC 유량 제어가 불안정해지며 실제 유량 측정값에 편차가 발생한 것으로 판단됩니다.',
        4: 'VG12 측정값의 드리프트·응답 지연·오염으로 인해 챔버 내 실제 압력이 정확히 반영되지 않아 공정 불안정이 발생한 것으로 확인되었습니다. 실제 Gas 유량변화에 따른 현상이므로 전체 압력을 변화시킨 원인을 제거해야 합니다. 1) VG12 Gauge 상태 확인: Base 압력 변화 확인, Gas 유량 대비 Gauge 값 비교 점검 2) Gas유량의 변화: Gas유량의 변화를 점검함(MFC상태점검, Gas 압력센서 점검)'
    };
    const label = entry?.param || 'MFC';
    const causeText = causeTexts[vt] || '원인 정보가 없습니다.';
    body.innerHTML = `
        <div class="report-cause-layout">
            <div class="report-cause-chart">
                <div class="report-cause-title">${label} 유량 추이 (±5초)</div>
                <canvas id="report-cause-canvas" aria-label="${label} 이상 구간 차트"></canvas>
            </div>
            <div class="report-cause-text">
                <div class="report-cause-badge">원인 진단</div>
                <div class="report-cause-lines">
                    <p>${headline}</p>
                    <p>${causeText}</p>
                </div>
            </div>
        </div>
    `;
    const canvas = document.getElementById('report-cause-canvas');
    await drawReportChart(entry, canvas);
}

function renderMfcActionTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    const fileMap = {
        1: 'MFC.html',
        2: 'Baratron_Guage.html',
        3: 'Magnetic_Seal.html',
        4: 'CKD 대신 VG12로 부품변경.html'
    };
    const file = fileMap[Number(entry?.violation_type)] || 'MFC.html';
    const label = entry?.param || 'MFC';
    body.innerHTML = `
        <div class="report-action-block">
            <div style="font-weight:700; margin-bottom:6px;">조치 방법 (${label})</div>
            <div class="report-markdown" id="mfc-md-container">불러오는 중...</div>
        </div>
    `;
    const container = document.getElementById('mfc-md-container');
    const encoded = encodeURIComponent(file);
    loadHtml(`/static/md/${encoded}`, `action-${file}`)
        .then(text => renderHtml(container, text))
        .catch(err => {
            console.error('failed to render html', err);
            if (container) container.textContent = '조치 방법 문서를 불러오지 못했습니다.';
        });
}

function renderMfcDrawingTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    const label = entry?.param || 'MFC';
    // 경량화된 STL 사용 (STEP은 브라우저에서 바로 로드 불가)
    const encoded = encodeURIComponent('ALD_20.stl');
    const modelUrl = `/static/3D/${encoded}`;
    body.innerHTML = `
        <div class="report-3d-wrap">
            <div class="report-3d-header">
                <span>도면 (3D)</span>
                <span style="font-size:13px; color:#475569;">파일: ALD_20.stl</span>
            </div>
            <div class="report-3d-canvas" id="report-3d-container">로딩 중...</div>
            <div class="report-3d-note">마우스 드래그: 회전 · 스크롤: 줌 · 오른쪽 버튼: 이동</div>
        </div>
    `;
    const container = document.getElementById('report-3d-container');
    createThreeViewer(container, modelUrl);
}

async function setActiveReportTab(tabKey) {
    const tabs = document.querySelectorAll('.report-tab');
    tabs.forEach(t => {
        if (t.dataset.tab === tabKey) t.classList.add('active');
        else t.classList.remove('active');
    });
    const body = reportElements.reportBody;
    if (!body || !currentReportEntry) return;
    const base = `${currentReportEntry.param} 관련 리포트가 준비되지 않았습니다.`;
    const defaultMap = {
        parts: '부품 확인 정보가 없습니다.',
        drawing: '도면(이상 위치) 정보가 없습니다.',
        process: '공정 단계 설명이 없습니다.'
    };
    try {
        if (tabKey === 'cause') {
            await renderMfcCauseTab(currentReportEntry);
        } else if (tabKey === 'action') {
            renderMfcActionTab(currentReportEntry);
        } else if (tabKey === 'drawing') {
            if (mfcParams.has(currentReportEntry.param)) {
                renderMfcDrawingTab(currentReportEntry);
            } else {
                destroyReportChart();
                disposeThreeViewer();
                body.textContent = defaultMap[tabKey] || base;
            }
        } else {
            destroyReportChart();
            disposeThreeViewer();
            body.textContent = defaultMap[tabKey] || base;
        }
    } catch (err) {
        console.error('failed to render report tab', err);
        body.textContent = '리포트 내용을 불러오지 못했습니다.';
    }
}

function openReportModal(entry) {
    if (!reportElements.reportModal || !reportElements.reportSummary || !reportElements.reportBody) return;
    currentReportEntry = entry;
    const timeText = formatTimelineTime(entry.end);
    const diff = entry.diff != null ? Math.abs(entry.diff).toFixed(0) : '0';
    const severity = buildSeverity(Number(diff));
    const logText = buildLogText(entry.param, entry);
    const summaryLines = [
        '이상 요약 (Summary)',
        `- 이상 감지 시간 : ${timeText}`,
        `- 이상 유형 : ${logText}`,
        `- 심각도 : ${severity.toUpperCase()}`
    ];
    reportElements.reportSummary.textContent = summaryLines.join('\n');
    setActiveReportTab('cause');
    reportElements.reportModal.style.display = 'flex';
}

function closeReportModal() {
    if (reportElements.reportModal) reportElements.reportModal.style.display = 'none';
    destroyReportChart();
    disposeThreeViewer();
    currentReportEntry = null;
}

async function fetchAbnormalLogs() {
    try {
        const res = await fetch('/api/logs');
        if (!res.ok) throw new Error('bad response');
        const data = await res.json();
        setServerLogs(Array.isArray(data) ? data : []);
    } catch (e) {
        console.error('failed to fetch abnormal logs', e);
    }
}

function fetchData() {
    if (!processStart) return;
    const now = new Date();
    const processStartTime = new Date(processStart).getTime();
    const thirtyMinAgo = now.getTime() - 300000; // 10 minutes
    const startIso = new Date(Math.max(processStartTime, thirtyMinAgo)).toISOString();
    const nowIso = now.toISOString();
    columns.forEach(col => {
        fetch(`/api/event_chart?param=${encodeURIComponent(col)}&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(nowIso)}`)
            .then(res => res.json())
            .then(json => updateCharts(col, json));
    });
}

function checkProcess() {
    fetch(`/api/process_range?time=${encodeURIComponent(new Date().toISOString())}`)
        .then(res => res.json())
        .then(r => {
            if (!processStart) {
                processStart = r.start;
                processEnd = r.end;
            } else if (processStart !== r.start) {
                processStart = r.start;
                processEnd = r.end;
                logs = {}; loggedIds.clear();
                updateLog();
                Object.values(processCharts).forEach(c => { c.data.datasets.forEach(ds => ds.data = []); c.options.plugins.highlightRegion.regions = []; c.update(); });
                Object.values(recentCharts).forEach(c => { c.data.datasets.forEach(ds => ds.data = []); c.options.plugins.highlightRegion.regions = []; c.update(); });
            } else {
                processEnd = r.end;
            }
        });
}

window.addEventListener('DOMContentLoaded', () => {
    loadSettings().finally(() => {
        const warningToggle = document.getElementById('warning-toggle');
        const stored = localStorage.getItem(warningToggleKey);
        if (settingsCache && typeof settingsCache.warning_enabled === 'boolean') {
            warningEnabled = settingsCache.warning_enabled;
        } else if (stored !== null) {
            warningEnabled = stored === 'true';
        }
        if (warningToggle) warningToggle.checked = warningEnabled;
        localStorage.setItem(warningToggleKey, String(warningEnabled));
    });
    createCharts();
    updateLogPanelHeight();
    checkProcess();
    //setInterval(checkProcess, 20000);
    setInterval(fetchData, 1000);
    fetchAbnormalLogs();
    setInterval(fetchAbnormalLogs, 2000);
    pollGeneratorStatus();
    setInterval(pollGeneratorStatus, 3000);
    fetchCurrentStepFallback(true);
    setInterval(() => fetchCurrentStepFallback(false), 5000);
    const btn = document.getElementById('toggle-datasets');
    const updateBtn = () => { btn.textContent = visibilityLabels[visibilityMode]; };
    updateBtn();
    btn.addEventListener('click', () => {
        visibilityMode = visibilityMode === 'both' ? 'actual' : visibilityMode === 'actual' ? 'predicted' : 'both';
        localStorage.setItem(visibilityKey, visibilityMode);
        updateBtn();
        applyVisibilityAll();
    });

    document.querySelectorAll('.expand-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            openChartModal(btn.dataset.param, btn.dataset.kind);
        });
    });

    const modal = document.getElementById('chart-modal');
    const closeBtn = document.getElementById('close-modal');
    if (closeBtn) closeBtn.addEventListener('click', closeChartModal);
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) closeChartModal();
        });
    }

    warningElements = {
        warningModal: document.getElementById('warning-modal'),
        warningParam: document.getElementById('warning-param'),
        warningClose: document.getElementById('warning-close'),
        warningCancel: document.getElementById('warning-cancel'),
        warningDown: document.getElementById('warning-down'),
        confirmModal: document.getElementById('confirm-modal'),
        confirmClose: document.getElementById('confirm-close'),
        confirmNo: document.getElementById('confirm-no'),
        confirmYes: document.getElementById('confirm-yes')
    };
    reportElements = {
        reportModal: document.getElementById('report-modal'),
        reportClose: document.getElementById('report-close'),
        reportSummary: document.getElementById('report-summary'),
        reportBody: document.getElementById('report-body')
    };

    if (warningElements.warningClose) warningElements.warningClose.addEventListener('click', closeWarningModal);
    if (warningElements.warningCancel) warningElements.warningCancel.addEventListener('click', closeWarningModal);
    if (warningElements.warningDown) warningElements.warningDown.addEventListener('click', openConfirmModal);
    if (warningElements.confirmClose) warningElements.confirmClose.addEventListener('click', closeConfirmModal);
    if (warningElements.confirmNo) warningElements.confirmNo.addEventListener('click', closeConfirmModal);
    if (warningElements.confirmYes) warningElements.confirmYes.addEventListener('click', () => {
        closeConfirmModal();
        closeWarningModal();
    });
    if (reportElements.reportClose) reportElements.reportClose.addEventListener('click', closeReportModal);
    if (reportElements.reportModal) {
        reportElements.reportModal.addEventListener('click', (e) => {
            if (e.target === reportElements.reportModal) closeReportModal();
        });
    }
    document.querySelectorAll('.report-tab').forEach(tab => {
        tab.addEventListener('click', () => setActiveReportTab(tab.dataset.tab));
    });

    // 경고 팝업 토글
    const warningToggle = document.getElementById('warning-toggle');
    const stored = localStorage.getItem(warningToggleKey);
    if (stored !== null) warningEnabled = stored === 'true';
    if (warningToggle) warningToggle.checked = warningEnabled;
    if (warningToggle) {
        warningToggle.addEventListener('change', () => {
            warningEnabled = warningToggle.checked;
            localStorage.setItem(warningToggleKey, String(warningEnabled));
            saveWarningSetting();
            if (!warningEnabled && warningModalOpen) closeWarningModal();
        });
    }
});

window.addEventListener('resize', updateLogPanelHeight);
window.addEventListener('resize', () => {
    if (modalChart) modalChart.resize();
});
