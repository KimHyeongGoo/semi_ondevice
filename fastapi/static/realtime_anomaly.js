const processCharts = {};
const recentCharts = {};
const chartBoxesMap = {};
const logEntryElements = new Map();
let processStart = null;
let processEnd = null;
// logs structure: { param: [ {start, end, diff, actual_value, predicted_value, peak_time, step_id, step_name} ] }
let logs = {};
const loggedIds = new Set();
let currentHighlight = null;

let latestStepTimestamp = 0;
let lastStepUpdate = 0;
let lastStepFallbackFetch = 0;

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
    if (!force && now - lastStepUpdate < 5000) {
        return;
    }
    if (!force && now - lastStepFallbackFetch < 5000) {
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
    return String(ts).slice(11, 19);
}

function safeId(name) {
    return name.replace(/[ .-]/g, '_');
}

function createCharts() {
    const xAxis = {
        type: 'time',
        time: { tooltipFormat: 'HH:mm:ss' },
        ticks: {
            callback: function (value) {
                return formatTime(value);
            }
        }
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
                    { label: '예측값', borderColor: 'red', tension: 0.3, data: [] },
                    { label: '실제값', borderColor: 'blue', tension: 0.3, data: [] }
                ]
            },
            options: { animation: false, plugins: { highlightRegion: { regions: [] } }, scales: { x: xAxis, y: {} } }
        });
        recentCharts[col] = new Chart(rctx, {
            type: 'line',
            data: {
                datasets: [
                    { label: '예측값', borderColor: 'red', tension: 0.3, data: [] },
                    { label: '실제값', borderColor: 'blue', tension: 0.3, data: [] }
                ]
            },
            options: { animation: false, plugins: { highlightRegion: { regions: [] } }, scales: { x: xAxis, y: {} } }
        });
        setDatasetVisibility(processCharts[col], visibilityMode);
        setDatasetVisibility(recentCharts[col], visibilityMode);
    });
    applyVisibilityAll();
    applyHighlightState();
}

function calcSegments(actual, predicted) {
    const predMap = new Map(predicted.map(d => [d.x, d.y]));
    let segStart = null;
    let last = null;
    let maxDiff = 0;
    let maxActual = null;
    let maxPred = null;
    let maxTime = null;
    const segments = [];
    const regions = [];
    actual.forEach(a => {
        const pv = predMap.get(a.x);
        const t = new Date(a.x).getTime();
        if (!Number.isFinite(t)) {
            return;
        }
        if (pv === undefined) {
            if (segStart !== null && last - segStart >= 4000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff, actual_value: maxActual, predicted_value: maxPred, peak_time: maxTime });
            }
            segStart = null;
            maxDiff = 0;
            maxActual = null;
            maxPred = null;
            maxTime = null;
            last = t;
            return;
        }
        const absDiff = Math.abs(a.y - pv);
        const diff = Math.abs(a.y - pv) / (Math.abs(a.y) || 1) * 100;
        if (diff > 10 && absDiff > 0.25) {
            if (segStart === null) {
                segStart = t;
                maxDiff = diff;
                maxActual = a.y;
                maxPred = pv;
                maxTime = t;
            } else if (diff > maxDiff) {
                maxDiff = diff;
                maxActual = a.y;
                maxPred = pv;
                maxTime = t;
            }
        } else if (segStart !== null) {
            if (last - segStart >= 4000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff, actual_value: maxActual, predicted_value: maxPred, peak_time: maxTime });
            }
            segStart = null;
            maxDiff = 0;
            maxActual = null;
            maxPred = null;
            maxTime = null;
        }
        last = t;
    });
    if (segStart !== null && last - segStart >= 4000) {
        regions.push({ start: segStart, end: last });
        segments.push({ start: segStart, end: last, max: maxDiff, actual_value: maxActual, predicted_value: maxPred, peak_time: maxTime });
    }
    return { segments, regions };
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
    if (key && logEntryElements.has(key)) {
        logEntryElements.get(key).classList.add('highlight');
    }
}

function setHighlight(param, key, options = {}) {
    if (currentHighlight && currentHighlight.param === param && currentHighlight.key === key) {
        currentHighlight = null;
    } else {
        currentHighlight = { param, key };
    }
    applyHighlightState();
    if (options.scroll && currentHighlight && currentHighlight.key) {
        const el = logEntryElements.get(currentHighlight.key);
        if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

function handleLogClick(param, key) {
    setHighlight(param, key);
}

function handleChartClick(param) {
    const entries = logs[param];
    if (!entries || !entries.length) {
        setHighlight(param, null);
        return;
    }
    const latest = entries[0];
    const key = getEntryKey(param, latest);
    setHighlight(param, key, { scroll: true });
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
        const bodyEl = document.createElement('div');
        bodyEl.className = 'timeline-body';
        bodyEl.appendChild(timeEl);
        bodyEl.appendChild(textEl);
        wrapper.appendChild(iconEl);
        wrapper.appendChild(bodyEl);
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


function addLogs(param, segments, actual) {
    if (!logs[param]) logs[param] = [];
    const newEntries = [];
    segments.forEach(s => {
        const key = `${param}-${s.start}-${s.end}`;
        const isDup = logs[param].some(l => Math.abs(l.start - s.start) < 2000 && Math.abs(l.end - s.end) < 2000) || loggedIds.has(key);
        if (!isDup) {
            const steps = actual.filter(a => {
                const t = new Date(a.x).getTime();
                return t >= s.start && t <= s.end && a.step_id != null;
            });
            const stepIds = [...new Set(steps.map(a => a.step_id))];
            const stepNames = [...new Set(steps.map(a => a.step_name).filter(Boolean))];
            const entry = {
                start: s.start,
                end: s.end,
                diff: s.max,
                step_id: stepIds,
                step_name: stepNames,
                actual_value: s.actual_value ?? null,
                predicted_value: s.predicted_value ?? null,
                peak_time: s.peak_time ?? null
            };
            logs[param].unshift(entry);
            logs[param].sort((a, b) => b.start - a.start);
            loggedIds.add(key);
            newEntries.push({ param, entry });
        }
    });
    trimLogEntries(20);
    updateLog();
    newEntries.forEach(({ param: p, entry }) => {
        const payload = {
            parameter: p,
            start: new Date(entry.start).toISOString(),
            end: new Date(entry.end).toISOString(),
            peak_time: entry.peak_time ? new Date(entry.peak_time).toISOString() : null,
            diff: entry.diff,
            duration_seconds: (entry.end - entry.start) / 1000,
            step_id: entry.step_id,
            step_name: entry.step_name,
            actual_value: entry.actual_value,
            predicted_value: entry.predicted_value
        };
        sendLogToServer(payload);
    });
}

function sendLogToServer(payload) {
    fetch('/api/logs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    }).catch(err => console.error('Failed to persist realtime log', err));
}

function updateCharts(col, data) {
    const actual = data.actual;
    const predicted = data.predicted;
    considerActualStepInfo(actual);
    const { segments, regions } = calcSegments(actual, predicted);
    const pChart = processCharts[col];
    pChart.data.datasets[0].data = predicted;
    pChart.data.datasets[1].data = actual;
    pChart.options.plugins.highlightRegion.regions = regions;
    const allTimestamps = actual.concat(predicted).map(d => new Date(d.x).getTime());
    if (allTimestamps.length) {
        pChart.options.scales.x.min = Math.min(...allTimestamps);
        pChart.options.scales.x.max = Math.max(...allTimestamps);
    }
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
    const { regions: reg2 } = calcSegments(aRecent, pRecent);
    const rChart = recentCharts[col];
    rChart.data.datasets[0].data = pRecent;
    rChart.data.datasets[1].data = aRecent;
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

    addLogs(col, segments, actual);
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
    createCharts();
    updateLogPanelHeight();
    checkProcess();
    //setInterval(checkProcess, 20000);
    setInterval(fetchData, 1000);
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
});

window.addEventListener('resize', updateLogPanelHeight);