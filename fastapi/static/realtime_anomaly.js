const processCharts = {};
const recentCharts = {};
let processStart = null;
let processEnd = null;
// logs structure: { param: [ {start, end, diff} ] }
let logs = {};
const loggedIds = new Set();

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

function formatLocal(ts) {
    const d = new Date(ts);
    return d.toLocaleString('sv').replace('T', ' ');
}

function formatTime(ts) {
    return String(ts).slice(11, 19);
}

function safeId(name) {
    return name.replace(/[ .-]/g, '_');
}

function fallbackCopy(text) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.top = '-1000px';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
}

function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
    } else {
        fallbackCopy(text);
    }
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
}

function calcSegments(actual, predicted) {
    const predMap = new Map(predicted.map(d => [d.x, d.y]));
    let segStart = null, last = null, maxDiff = 0;
    const segments = [], regions = [];
    actual.forEach(a => {
        const pv = predMap.get(a.x);
        const t = new Date(a.x).getTime();
        if (pv === undefined) {
            if (segStart !== null && last - segStart >= 4000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff });
            }
            segStart = null; maxDiff = 0; last = t;
            return;
        }
        const absDiff = Math.abs(a.y - pv);
        const diff = Math.abs(a.y - pv) / (Math.abs(a.y) || 1) * 100;
        if (diff > 10 && absDiff > 0.25) {
            if (segStart === null) { segStart = t; maxDiff = diff; }
            else { maxDiff = Math.max(maxDiff, diff); }
        } else if (segStart !== null) {
            if (last - segStart >= 4000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff });
            }
            segStart = null; maxDiff = 0;
        }
        last = t;
    });
    if (segStart !== null && last - segStart >= 4000) {
        regions.push({ start: segStart, end: last });
        segments.push({ start: segStart, end: last, max: maxDiff });
    }
    return { segments, regions };
}

function updateLog() {
    const logDiv = document.getElementById('log-content');
    logDiv.innerHTML = '';
    const params = Object.keys(logs).sort((a, b) => {
        const la = logs[a][0]?.start || 0;
        const lb = logs[b][0]?.start || 0;
        return lb - la; // most recent param first
    });

    params.forEach(p => {
        const header = document.createElement('div');
        header.className = 'param-label';
        header.textContent = p;
        header.style.cursor = 'pointer';
        header.addEventListener('click', () => copyText(p));
        logDiv.appendChild(header);
        logs[p].forEach(l => {
            const dur = Math.round((l.end - l.start) / 1000);
            const lines = [
                '{',
                `"parameter" : "${p}",`,
                `"start" : "${formatLocal(l.start)}",`,
                `"end" : "${formatLocal(l.end)}",`,
                `"duration" : ${dur},`,
                `"diff" : ${l.diff.toFixed(2)}%,`,
                `"step_id" : "[${l.step_id.join(', ')}]",`,
                `"step_name" : "[${l.step_name.join(', ')}]"`,
                '}'
            ];
            const text = lines.join('\n');
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            const pre = document.createElement('pre');
            pre.textContent = text;
            const btn = document.createElement('button');
            btn.className = 'copy-btn';
            btn.textContent = 'Copy';
            btn.addEventListener('click', () => copyText(text));
            entry.appendChild(pre);
            entry.appendChild(btn);
            logDiv.appendChild(entry);
        });
    });
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
            step_name: l.step_name
        });
        loggedIds.add(`${l.param}-${l.start}-${l.end}`);
    });
}


function addLogs(param, segments, actual) {
    if (!logs[param]) logs[param] = [];
    segments.forEach(s => {
        const isDup = logs[param].some(l => Math.abs(l.start - s.start) < 2000 && Math.abs(l.end - s.end) < 2000);
        if (!isDup) {
            const steps = actual.filter(a => {
                const t = new Date(a.x).getTime();
                return t >= s.start && t <= s.end && a.step_id != null;
            });
            const stepIds = [...new Set(steps.map(a => a.step_id))];
            const stepNames = [...new Set(steps.map(a => a.step_name).filter(Boolean))];
            logs[param].unshift({
                start: s.start,
                end: s.end,
                diff: s.max,
                step_id: stepIds,
                step_name: stepNames
            });
            loggedIds.add(`${param}-${s.start}-${s.end}`);
        }
    });
    trimLogEntries(20);
    updateLog();
}

function updateCharts(col, data) {
    const actual = data.actual;
    const predicted = data.predicted;
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
    setInterval(checkProcess, 20000);
    setInterval(fetchData, 1000);
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