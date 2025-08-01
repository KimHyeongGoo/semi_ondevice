const processCharts = {};
const recentCharts = {};
let processStart = null;
let processEnd = null;
// logs structure: { param: [ {start, end, diff} ] }
let logs = {};
const loggedIds = new Set();

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

function iso(t) { return new Date(t).toISOString(); }
function formatLocal(t) { const d = new Date(t); return d.toLocaleString('sv').replace('T', ' '); }

function createCharts() {
    columns.forEach(col => {
        const pctx = document.getElementById(`proc-${col}`).getContext('2d');
        const rctx = document.getElementById(`recent-${col}`).getContext('2d');
        processCharts[col] = new Chart(pctx, { type: 'line', data: { datasets: [{ label: '예측값', borderColor: 'red', tension: 0.3, data: [] }, { label: '실제값', borderColor: 'blue', tension: 0.3, data: [] }] }, options: { animation: false, plugins: { highlightRegion: { regions: [] } }, scales: { x: { type: 'time', time: { tooltipFormat: 'HH:mm:ss' } }, y: {} } } });
        recentCharts[col] = new Chart(rctx, { type: 'line', data: { datasets: [{ label: '예측값', borderColor: 'red', tension: 0.3, data: [] }, { label: '실제값', borderColor: 'blue', tension: 0.3, data: [] }] }, options: { animation: false, plugins: { highlightRegion: { regions: [] } }, scales: { x: { type: 'time', time: { tooltipFormat: 'HH:mm:ss' } }, y: {} } } });
    });
}

function calcSegments(actual, predicted) {
    const predMap = new Map(predicted.map(d => [d.x, d.y]));
    let segStart = null, last = null, maxDiff = 0;
    const segments = [], regions = [];
    actual.forEach(a => {
        const pv = predMap.get(a.x);
        const t = new Date(a.x).getTime();
        if (pv === undefined) {
            if (segStart !== null && last - segStart >= 10000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff });
            }
            segStart = null; maxDiff = 0; last = t;
            return;
        }
        const diff = Math.abs(a.y - pv) / (Math.abs(a.y) || 1) * 100;
        if (diff > 10) {
            if (segStart === null) { segStart = t; maxDiff = diff; }
            else { maxDiff = Math.max(maxDiff, diff); }
        } else if (segStart !== null) {
            if (last - segStart >= 10000) {
                regions.push({ start: segStart, end: last });
                segments.push({ start: segStart, end: last, max: maxDiff });
            }
            segStart = null; maxDiff = 0;
        }
        last = t;
    });
    if (segStart !== null && last - segStart >= 10000) {
        regions.push({ start: segStart, end: last });
        segments.push({ start: segStart, end: last, max: maxDiff });
    }
    return { segments, regions };
}

function updateLog() {
    const logDiv = document.getElementById('log-content');
    const params = Object.keys(logs).sort((a, b) => {
        const la = logs[a][0]?.start || 0;
        const lb = logs[b][0]?.start || 0;
        return lb - la; // most recent param first
    });
    const lines = [];
    params.forEach(p => {
        lines.push(p);
        logs[p].forEach((l, idx) => {
            const dur = Math.round((l.end - l.start) / 1000);
            lines.push('{');
            lines.push(`start : ${formatLocal(l.start)},`);
            lines.push(`end : ${formatLocal(l.end)},`);
            lines.push(`diff : ${l.diff.toFixed(2)}%,`);
            lines.push(`duration : ${dur}초`);
            lines.push('}' + (idx < logs[p].length - 1 ? ',' : ''));
        });
    });
    logDiv.textContent = lines.join('\n');
}

function addLogs(param, segments) {
    if (!logs[param]) logs[param] = [];
    segments.forEach(s => {
        const id = `${param}-${s.start}-${s.end}`;
        if (!loggedIds.has(id)) {
            loggedIds.add(id);
            logs[param].unshift({ start: s.start, end: s.end, diff: s.max });
        }
    });
    updateLog();
}

function updateCharts(col, data) {
    const { segments, regions } = calcSegments(data.actual, data.predicted);
    const pChart = processCharts[col];
    pChart.data.datasets[0].data = data.predicted.map(d => ({ x: d.x, y: d.y }));
    pChart.data.datasets[1].data = data.actual.map(d => ({ x: d.x, y: d.y }));
    pChart.options.plugins.highlightRegion.regions = regions;
    pChart.update();

    const recentStart = Date.now() - 120000;
    const aRecent = data.actual.filter(d => new Date(d.x).getTime() >= recentStart);
    const pRecent = data.predicted.filter(d => new Date(d.x).getTime() >= recentStart);
    const { regions: reg2 } = calcSegments(aRecent, pRecent);
    const rChart = recentCharts[col];
    rChart.data.datasets[0].data = pRecent.map(d => ({ x: d.x, y: d.y }));
    rChart.data.datasets[1].data = aRecent.map(d => ({ x: d.x, y: d.y }));
    rChart.options.plugins.highlightRegion.regions = reg2;
    rChart.update();

    addLogs(col, segments);
}

function fetchData() {
    if (!processStart) return;
    const now = new Date();
    const processStartTime = new Date(processStart).getTime();
    const thirtyMinAgo = now.getTime() - 600000; // 30 minutes
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
    checkProcess();
    setInterval(checkProcess, 5000);
    setInterval(fetchData, 1000);
});