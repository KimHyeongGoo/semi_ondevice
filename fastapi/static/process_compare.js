let predictColumns = [];
let allProcesses = [];
let pageIndex = 0;
let pageSize = 0;
let selectedStart = null;

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
        selectProcess(container.querySelector('.process-box'));
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
    loadCharts(box.dataset.start, box.dataset.end);
}

async function loadCharts(start, end) {
    const container = document.getElementById('charts-container');
    container.innerHTML = '';
    predictColumns.forEach(col => {
        const chartBox = document.createElement('div');
        chartBox.className = 'chart-box';
        chartBox.innerHTML = `<h4>${col}</h4><canvas id="chart-${col}"></canvas>`;
        container.appendChild(chartBox);
        fetch(`/api/trace_pred_chart?param=${encodeURIComponent(col)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`)
            .then(res => res.json())
            .then(json => {
                const all = json.actual.concat(json.predicted);
                const vals = all.map(d => d.y ?? d.value);
                const yMin = Math.min(...vals);
                const yMax = Math.max(...vals);
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
                        scales: {
                            x: { type: 'time', time: { tooltipFormat: 'HH:mm:ss' } },
                            y: { min: yMin, max: yMax }
                        }
                    }
                });
            });
    });
}

window.addEventListener('DOMContentLoaded', async () => {
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