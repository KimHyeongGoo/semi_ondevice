let predictColumns = [];
let allProcesses = [];
let pageIndex = 0;
let pageSize = 0;
let selectedStart = null;
let diffInfoDiv, chartsContainerEl, infoAreaEl;
let loadId = 0;

function formatLocal(ts) {
    const d = new Date(ts);
    const p = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

const highlightPlugin = {
    id: 'highlightRegion',
    beforeDatasetsDraw(chart, args, opts) {
        const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
        ctx.save();
        ctx.fillStyle = 'rgba(173, 255, 47, 0.4)';
        (opts.regions || []).forEach(r => {
            const xStart = x.getPixelForValue(r.start);
            const xEnd = x.getPixelForValue(r.end);
            ctx.fillRect(xStart, top, xEnd - xStart, bottom - top);
        });
        ctx.restore();
    }
};
Chart.register(highlightPlugin);

function updateInfoHeight() {
    infoAreaEl.style.maxHeight = chartsContainerEl.offsetHeight + 'px';
}

async function loadColumns() {
    const res = await fetch('/api/model_columns');
    predictColumns = await res.json();
}

async function loadProcesses() {
    const res = await fetch('/api/trace_info?limit=50');
    allProcesses = (await res.json()).reverse();
    updateLayout();
}

function calcLayout() {
    const container = document.getElementById('process-container');
    const boxWidth = 220 + 16;
    const cols = Math.max(1, Math.floor(container.clientWidth / boxWidth));
    pageSize = cols;
}


function renderPage() {
    const container = document.getElementById('process-container');
    container.innerHTML = '';
    const start = pageIndex * pageSize;
    const slice = allProcesses.slice(start, start + pageSize);
    slice.forEach(proc => {
        const box = document.createElement('div');
        box.className = 'process-box';
        box.innerHTML = `
            <div><strong>공정 ${proc.row_num}</strong></div>
            <div>${proc.start_time}</div>
            <div>${proc.end_time}</div>
        `;
        box.dataset.start = proc.start_time;
        box.dataset.end = proc.end_time;
        if (proc.start_time === selectedStart) box.classList.add('active');
        box.addEventListener('click', () => selectProcess(box));
        container.appendChild(box);
    });
    if (!selectedStart && slice.length > 0) {
        selectProcess(container.lastElementChild);
    }
    document.getElementById('prevPage').disabled = pageIndex === 0;
    const totalPages = Math.ceil(allProcesses.length / pageSize);
    document.getElementById('nextPage').disabled = pageIndex >= totalPages - 1;
}

function updateLayout() {
    const prevSize = pageSize;
    calcLayout();
    const totalPages = Math.ceil(allProcesses.length / pageSize);
    if (pageIndex > totalPages - 1) pageIndex = Math.max(0, totalPages - 1);
    if (prevSize !== pageSize) {
        pageIndex = totalPages - 1;
    }
    renderPage();
}

function selectProcess(box) {
    document.querySelectorAll('.process-box').forEach(b => b.classList.remove('active'));
    box.classList.add('active');
    diffInfoDiv.innerHTML = '';
    loadCharts(box.dataset.start, box.dataset.end);
}

async function loadCharts(start, end) {
    const current = ++loadId;
    chartsContainerEl.innerHTML = '';

    predictColumns.forEach(col => {

        fetch(`/api/trace_pred_chart?param=${encodeURIComponent(col)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`)
            .then(res => res.json())
            .then(json => {
                if (current !== loadId) return;
                const predMap = new Map(json.predicted.map(d => [d.x, d.y]));
                let segStart = null;
                let lastTime = null;
                let maxDiff = 0;
                const segments = [];
                const regions = [];

                json.actual.forEach(a => {
                    const pv = predMap.get(a.x);
                    const t = new Date(a.x).getTime();
                    if (pv === undefined) {
                        if (segStart !== null && (lastTime - segStart) >= 10000) {
                            regions.push({ start: segStart, end: lastTime });
                            segments.push({ start: formatLocal(segStart), end: formatLocal(lastTime), max: maxDiff });
                        }
                        segStart = null; maxDiff = 0; return;
                    }
                    const diffPct = Math.abs(a.y - pv) / (Math.abs(a.y) || 10) * 100;
                    if (diffPct > 10) {
                        if (segStart === null) { segStart = t; maxDiff = diffPct; }
                        else { maxDiff = Math.max(maxDiff, diffPct); }
                    } else if (segStart !== null) {
                        if (lastTime !== null && (lastTime - segStart) >= 10000) {
                            regions.push({ start: segStart, end: lastTime });
                            segments.push({ start: formatLocal(segStart), end: formatLocal(lastTime), max: maxDiff });
                        }
                        segStart = null; maxDiff = 0;
                    }
                    lastTime = t;
                });

                if (segStart !== null && lastTime - segStart >= 10000) {
                    regions.push({ start: segStart, end: lastTime });
                    segments.push({ start: formatLocal(segStart), end: formatLocal(lastTime), max: maxDiff });
                }
                if (segments.length === 0) return;

                const chartBox = document.createElement('div');
                chartBox.className = 'chart-box';
                chartBox.innerHTML = `<h4>${col}</h4><canvas id="chart-${col}" width="100%"></canvas>`;
                chartsContainerEl.appendChild(chartBox);
                updateInfoHeight();

                const yValues = json.actual.concat(json.predicted).map(d => d.y ?? d.value);
                const yMin = Math.min(...yValues);
                const yMax = Math.max(...yValues);

                new Chart(document.getElementById(`chart-${col}`), {
                    type: 'line',
                    data: {
                        datasets: [
                            { label: '예측값', data: json.predicted.map(d => ({ x: d.x, y: d.y })), borderColor: 'red', tension: 0.3 },
                            { label: '실제값', data: json.actual.map(d => ({ x: d.x, y: d.y })), borderColor: 'blue', tension: 0.3 }
                        ]
                    },
                    options: {
                        animation: false,
                        plugins: { highlightRegion: { regions } },
                        scales: {
                            x: { type: 'time', time: { tooltipFormat: 'HH:mm:ss' } },
                            y: { min: yMin, max: yMax }
                        }
                    }
                });

                const infoDiv = document.createElement('div');
                infoDiv.innerHTML = `<h4>${col}</h4>` + segments.map(s => {
                    const dur = (new Date(s.end) - new Date(s.start)) / 1000;
                    return `<div>{<br>start : ${s.start},<br>end : ${s.end},<br>diff : ${(s.max).toFixed(2)}%,<br>duration : ${dur.toFixed(0)}초<br>},</div>`;
                }).join('');
                diffInfoDiv.appendChild(infoDiv);
            });
    });
}

window.addEventListener('DOMContentLoaded', async () => {
    diffInfoDiv = document.getElementById('diff-info');
    chartsContainerEl = document.getElementById('charts-container');
    infoAreaEl = document.getElementById('info-area');
    new ResizeObserver(updateInfoHeight).observe(chartsContainerEl);
    await loadColumns();
    await loadProcesses();
    window.addEventListener('resize', updateLayout);
    document.getElementById('prevPage').addEventListener('click', () => {
        if (pageIndex > 0) {
            pageIndex--;
            renderPage();
        }
    });
    document.getElementById('nextPage').addEventListener('click', () => {
        const totalPages = Math.ceil(allProcesses.length / pageSize);
        if (pageIndex < totalPages - 1) {
            pageIndex++;
            renderPage();
        }
    });
});