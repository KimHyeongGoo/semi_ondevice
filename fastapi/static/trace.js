function format(val) {
    if (val === null || val === undefined || isNaN(val)) return '';
    return parseFloat(val).toFixed(2);
}

let lastStepInfoFetch = 0;

function updateCurrentStepDisplay(stepId, stepName) {
    const idEl = document.getElementById('current-step-id');
    const nameEl = document.getElementById('current-step-name');
    if (idEl) {
        idEl.textContent = stepId !== null && stepId !== undefined ? stepId : '-';
    }
    if (nameEl) {
        nameEl.textContent = stepName ? stepName : '-';
    }
}

async function fetchCurrentStepInfo(force = false) {
    const now = Date.now();
    if (!force && now - lastStepInfoFetch < 5000) return;
    lastStepInfoFetch = now;
    try {
        const res = await fetch('/api/current_step');
        if (!res.ok) throw new Error('failed');
        const data = await res.json();
        updateCurrentStepDisplay(data.step_id ?? null, data.step_name ?? null);
    } catch (e) {
        updateCurrentStepDisplay(null, null);
    }
}

function buildTable(proc) {
    const table = document.createElement('table');
    table.className = 'thickness-table';

    const header = document.createElement('tr');
    header.innerHTML = '<th></th><th>U</th><th>CU</th><th>C</th><th>CL</th><th>L</th>';
    table.appendChild(header);

    const vals = proc.thicknesses;
    for (let i = 0; i < 9; i++) {
        const row = document.createElement('tr');
        let html = `<td>${i + 1}</td>`;
        for (let j = 0; j < 5; j++) {
            html += `<td>${format(vals[i * 5 + j])}</td>`;
        }
        row.innerHTML = html;
        table.appendChild(row);
    }

    const cols = [[], [], [], [], []];
    for (let i = 0; i < 9; i++) {
        for (let j = 0; j < 5; j++) {
            cols[j].push(vals[i * 5 + j]);
        }
    }
    const avg = cols.map(c => c.reduce((a, b) => a + b, 0) / c.length);
    const max = cols.map(c => Math.max(...c));
    const min = cols.map(c => Math.min(...c));
    const range = cols.map((c, idx) => max[idx] - min[idx]);
    const uf = cols.map((c, idx) => range[idx] / avg[idx] * 50);
    const summary = [
        ['average', avg],
        ['max', max],
        ['min', min],
        ['range', range],
        ['u/f', uf]
    ];
    for (const [name, arr] of summary) {
        const row = document.createElement('tr');
        let html = `<td>${name}</td>`;
        for (let j = 0; j < 5; j++) html += `<td>${format(arr[j])}</td>`;
        row.innerHTML = html;
        table.appendChild(row);
    }
    return table;
}

let allData = [];
let pageIndex = 0;
let pageSize = 0;

function calcLayout() {
    const container = document.getElementById('process-container');
    const boxWidth = 340 + 16; // width + gap
    const cols = Math.max(1, Math.floor(container.clientWidth / boxWidth));
    pageSize = cols * 2;
}

function renderPage() {
    const container = document.getElementById('process-container');
    container.innerHTML = '';
    const start = pageIndex * pageSize;
    const slice = allData.slice(start, start + pageSize);
    slice.forEach(proc => {
        const box = document.createElement('div');
        box.className = 'process-box';

        const header = document.createElement('div');
        header.className = 'proc-header';
        header.innerHTML = `
            <div class="proc-row">공정 ${proc.row_num}</div>
            <div class="proc-row"><span class="label">START :</span><span class="value">${proc.start_time}</span></div>
            <div class="proc-row"><span class="label">END :</span><span class="value">${proc.end_time}</span></div>
        `;
        box.appendChild(header);
        box.appendChild(buildTable(proc));
        container.appendChild(box);
    });
    document.getElementById('prevPage').disabled = pageIndex === 0;
    const totalPages = Math.ceil(allData.length / pageSize);
    document.getElementById('nextPage').disabled = pageIndex >= totalPages - 1;
}

function updateLayout() {
    const prevSize = pageSize;
    calcLayout();
    const totalPages = Math.ceil(allData.length / pageSize);
    if (pageIndex > totalPages - 1) pageIndex = Math.max(0, totalPages - 1);
    if (prevSize !== pageSize) {
        pageIndex = totalPages - 1;
    }
    renderPage();
}

async function fetchData() {
    const res = await fetch('/api/trace_info?limit=50');
    allData = (await res.json()).reverse();
    updateLayout();
    fetchCurrentStepInfo();
}

window.addEventListener('DOMContentLoaded', () => {
    fetchData();
    fetchCurrentStepInfo(true);
    setInterval(fetchCurrentStepInfo, 5000);
    window.addEventListener('resize', updateLayout);
    document.getElementById('prevPage').addEventListener('click', () => {
        if (pageIndex > 0) {
            pageIndex--;
            renderPage();
        }
    });
    document.getElementById('nextPage').addEventListener('click', () => {
        const totalPages = Math.ceil(allData.length / pageSize);
        if (pageIndex < totalPages - 1) {
            pageIndex++;
            renderPage();
        }
    });
});