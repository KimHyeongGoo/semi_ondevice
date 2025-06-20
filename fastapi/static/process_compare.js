let predictColumns = [];

async function loadColumns() {
    const res = await fetch('/api/model_columns');
    predictColumns = await res.json();
}

async function loadProcesses() {
    const res = await fetch('/api/trace_info?limit=20');
    const data = await res.json();
    renderProcessList(data);
    if (data.length > 0) {
        selectProcess(document.querySelector('.process-box'));
    }
}

function renderProcessList(processes) {
    const container = document.getElementById('process-list');
    container.innerHTML = '';
    processes.forEach(proc => {
        const box = document.createElement('div');
        box.className = 'process-box';
        box.innerHTML = `
            <div><strong>공정 ${proc.row_num}</strong></div>
            <div>${proc.start_time}</div>
            <div>${proc.end_time}</div>
        `;
        box.dataset.start = proc.start_time;
        box.dataset.end = proc.end_time;
        box.addEventListener('click', () => selectProcess(box));
        container.appendChild(box);
    });
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
        fetch(`/api/event_chart?param=${encodeURIComponent(col)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&step=10`)
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
});