function format(val) {
    if (val === null || val === undefined || isNaN(val)) return '';
    return parseFloat(val).toFixed(2);
}

let lastStepInfoFetch = 0;
const HEATMAP_LABELS = ['U', 'CU', 'C', 'CL', 'L'];

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
    let headerHtml = '<th></th>';
    HEATMAP_LABELS.forEach(label => {
        headerHtml += `<th>${label}</th>`;
    });
    header.innerHTML = headerHtml;
    table.appendChild(header);

    const heatmapRow = document.createElement('tr');
    const images = proc.heatmap_images || {};
    let heatmapHtml = '<td class="heatmap-index-cell">등고선</td>';
    HEATMAP_LABELS.forEach(label => {
        const src = images[label];
        if (src) {
            heatmapHtml += `<td><img src="${src}" alt="${label} wafer thickness contour" class="heatmap-cell-image" loading="lazy"></td>`;
        } else {
            heatmapHtml += '<td><div class="heatmap-placeholder">데이터 없음</div></td>';
        }
    });
    heatmapRow.innerHTML = heatmapHtml;
    table.appendChild(heatmapRow);

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

function buildSingleTableHtml(proc, labelIndex, label) {
    const vals = proc.thicknesses || [];
    const colVals = [];
    for (let i = 0; i < 9; i++) {
        colVals.push(vals[i * 5 + labelIndex]);
    }
    const avg = colVals.reduce((a, b) => a + b, 0) / colVals.length;
    const max = Math.max(...colVals);
    const min = Math.min(...colVals);
    const range = max - min;
    const uf = avg ? (range / avg * 50) : 0;

    let html = '<table class="thickness-table"><thead><tr><th></th><th>' + label + '</th></tr></thead><tbody>';
    for (let i = 0; i < 9; i++) {
        html += `<tr><td>${i + 1}</td><td>${format(colVals[i])}</td></tr>`;
    }
    const summary = [
        ['average', avg],
        ['max', max],
        ['min', min],
        ['range', range],
        ['u/f', uf]
    ];
    summary.forEach(([name, val]) => {
        html += `<tr><td>${name}</td><td>${format(val)}</td></tr>`;
    });
    html += '</tbody></table>';
    return html;
}

function openHeatmapModal(imgSrc, tableHtml) {
    const modal = document.getElementById('heatmap-modal');
    const imgWrap = document.getElementById('heatmap-modal-image');
    const tableWrap = document.getElementById('heatmap-modal-table');
    if (!modal || !imgWrap || !tableWrap) return;
    imgWrap.innerHTML = `<img src="${imgSrc}" alt="Wafer Map 확대">`;
    tableWrap.innerHTML = tableHtml;
    modal.style.display = 'flex';
}

function attachHeatmapClicks(scope, proc) {
    const images = scope.querySelectorAll('.heatmap-cell-image');
    images.forEach((img, idx) => {
        if (!HEATMAP_LABELS[idx]) return;
        img.addEventListener('click', () => {
            const tableHtml = buildSingleTableHtml(proc, idx, HEATMAP_LABELS[idx]);
            openHeatmapModal(img.src, tableHtml);
        });
    });
}

let allData = [];
let pageIndex = 0;
let pageSize = 2;
let filteredData = [];

function calcLayout() {
    // 고정 2개씩 표시
    pageSize = 2;
}

function renderPage() {
    const container = document.getElementById('process-container');
    container.innerHTML = '';
    if (pageSize <= 0) pageSize = 2;
    const start = pageIndex * pageSize;
    const slice = filteredData.slice(start, start + pageSize);
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
        const table = buildTable(proc);
        box.appendChild(table);
        attachHeatmapClicks(box, proc);
        container.appendChild(box);
    });
    document.getElementById('prevPage').disabled = pageIndex === 0;
    const totalPages = Math.max(1, Math.ceil(filteredData.length / pageSize));
    document.getElementById('nextPage').disabled = pageIndex >= totalPages - 1;
}

function updateLayout() {
    calcLayout();
    if (pageSize <= 0) pageSize = 2;
    const totalPages = Math.max(1, Math.ceil(filteredData.length / pageSize));
    if (pageIndex > totalPages - 1) pageIndex = Math.max(0, totalPages - 1);
    renderPage();
}

async function fetchData() {
    const res = await fetch('/api/trace_info?limit=300');
    const data = await res.json();
    data.sort((a, b) => new Date(a.start_time) - new Date(b.start_time)); // 오래된 -> 최신
    allData = data;
    filteredData = [...allData];
    calcLayout();
    pageIndex = 0; // 가장 오래된 공정부터 시작
    renderPage();
    fetchCurrentStepInfo();
}

function applyFilters() {
    const startNum = parseInt(document.getElementById('proc-start')?.value);
    const endNum = parseInt(document.getElementById('proc-end')?.value);
    const startDate = document.getElementById('date-start')?.value;
    const endDate = document.getElementById('date-end')?.value;

    filteredData = allData.filter(item => {
        let ok = true;
        if (!isNaN(startNum)) ok = ok && item.row_num >= startNum;
        if (!isNaN(endNum)) ok = ok && item.row_num <= endNum;
        if (startDate) ok = ok && new Date(item.start_time) >= new Date(startDate);
        if (endDate) ok = ok && new Date(item.end_time) <= new Date(endDate);
        return ok;
    });
    calcLayout();
    pageIndex = 0; // 필터 적용 후에도 첫 페이지(가장 오래된 공정)부터 표시
    renderPage();
}

function resetFilters() {
    const ps = document.getElementById('proc-start');
    const pe = document.getElementById('proc-end');
    const ds = document.getElementById('date-start');
    const de = document.getElementById('date-end');
    if (ps) ps.value = '';
    if (pe) pe.value = '';
    if (ds) ds.value = '';
    if (de) de.value = '';
    filteredData = [...allData];
    calcLayout();
    pageIndex = 0;
    renderPage();
}

window.addEventListener('DOMContentLoaded', () => {
    fetchData();
    fetchCurrentStepInfo(true);
    setInterval(fetchCurrentStepInfo, 5000);
    window.addEventListener('resize', updateLayout);
    document.getElementById('filter-apply')?.addEventListener('click', applyFilters);
    document.getElementById('filter-reset')?.addEventListener('click', resetFilters);
    document.getElementById('prevPage').addEventListener('click', () => {
        if (pageIndex > 0) {
            pageIndex--;
            renderPage();
        }
    });
    document.getElementById('nextPage').addEventListener('click', () => {
        if (pageSize <= 0) pageSize = 2;
        const totalPages = Math.max(1, Math.ceil(filteredData.length / pageSize));
        if (pageIndex < totalPages - 1) {
            pageIndex++;
            renderPage();
        }
    });
    const heatmapModal = document.getElementById('heatmap-modal');
    const heatmapClose = document.getElementById('heatmap-close');
    if (heatmapClose) heatmapClose.addEventListener('click', () => {
        if (heatmapModal) heatmapModal.style.display = 'none';
    });
    if (heatmapModal) {
        heatmapModal.addEventListener('click', (e) => {
            if (e.target === heatmapModal) heatmapModal.style.display = 'none';
        });
    }
});
