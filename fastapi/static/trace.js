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
        if (!res.ok) {
            const errorText = await res.text();
            console.error('current_step API 오류:', res.status, errorText);
            throw new Error('failed');
        }
        const contentType = res.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            const text = await res.text();
            console.error('JSON이 아닌 응답:', text.substring(0, 100));
            throw new Error('Invalid response format');
        }
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

function generateLLMQuery(proc) {
    const vals = proc.thicknesses || [];
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

    // 표 데이터 생성
    let tableData = '\tU\tCU\tC\tCL\tL\n';
    for (let i = 0; i < 9; i++) {
        tableData += `${i + 1}\t${format(vals[i * 5 + 0])}\t${format(vals[i * 5 + 1])}\t${format(vals[i * 5 + 2])}\t${format(vals[i * 5 + 3])}\t${format(vals[i * 5 + 4])}\n`;
    }
    tableData += `average\t${format(avg[0])}\t${format(avg[1])}\t${format(avg[2])}\t${format(avg[3])}\t${format(avg[4])}\n`;
    tableData += `max\t${format(max[0])}\t${format(max[1])}\t${format(max[2])}\t${format(max[3])}\t${format(max[4])}\n`;
    tableData += `min\t${format(min[0])}\t${format(min[1])}\t${format(min[2])}\t${format(min[3])}\t${format(min[4])}\n`;
    tableData += `range\t${format(range[0])}\t${format(range[1])}\t${format(range[2])}\t${format(range[3])}\t${format(range[4])}\n`;
    tableData += `u/f\t${format(uf[0])}\t${format(uf[1])}\t${format(uf[2])}\t${format(uf[3])}\t${format(uf[4])}\n`;

    const queryText = `${tableData}
다음 데이터는 ALD(Atomic Layer Deposition) 공정 완료 후 측정된 웨이퍼 박막 두께 결과이다.
본 측정은 웨이퍼 보트(Boat) 내 상·하 위치별 웨이퍼와 각 웨이퍼 표면의 9개 대표 측정 포인트를 기준으로 수행되었다.

1. 웨이퍼 보트 내 위치 정의
웨이퍼는 공정 중 웨이퍼 보트 내 수직 방향 위치에 따라 다음과 같이 구분된다.
U (Upper) : 웨이퍼 보트 최상단 위치
CU (Center-Upper) : 웨이퍼 보트 중상단 위치
C (Center) : 웨이퍼 보트 중앙 위치
CL (Center-Lower) : 웨이퍼 보트 중하단 위치
L (Lower) : 웨이퍼 보트 최하단 위치
각 위치는 공정 중 가스 유량 분포, 반응물 도달 특성, 온도 분포 차이의 영향을 받을 수 있는 구조적 위치를 의미한다.

2. 웨이퍼 내 측정 포인트 정의
각 웨이퍼의 박막 두께는 웨이퍼 표면 내 다음 9개 고정 측정 포인트에서 측정되었다.
Point 1 : 웨이퍼 정중앙(Center)
Point 2 : 중앙에서 상부 방향의 내부 영역(Upper-Center)
Point 3 : 중앙에서 좌측 방향의 내부 영역(Left-Center)
Point 4 : 중앙에서 하부 방향의 내부 영역(Lower-Center)
Point 5 : 중앙에서 우측 방향의 내부 영역(Right-Center)
Point 6 : 웨이퍼 상단 가장자리 부근(Top Edge)
Point 7 : 웨이퍼 좌측 가장자리 부근(Left Edge)
Point 8 : 웨이퍼 하단 가장자리 부근(Bottom Edge)
Point 9 : 웨이퍼 우측 가장자리 부근(Right Edge)
이와 같이 측정 포인트는 중앙 1점, 중앙 주변 내부 영역 4점, 외곽 가장자리 영역 4점으로 구성되어 있으며,
이를 통해 웨이퍼 전면의 중심부–외곽부 두께 분포 및 방향성 편차를 평가할 수 있다.

3. 데이터 구성 및 통계 지표 정의
각 웨이퍼 보트 위치(U, CU, C, CL, L)에 대해 다음 지표가 산출되었다.
average : 해당 위치 웨이퍼의 9개 측정 포인트 두께 평균값
max : 해당 위치 웨이퍼의 최대 두께값
min : 해당 위치 웨이퍼의 최소 두께값
range : 두께 범위 (max − min) → 웨이퍼 내 두께 편차의 절대적 크기 지표
u/f (uniformity) : 두께 균일도 지표로, 일반적으로

4. 요청 사항
상기 데이터를 기반으로 이번 ALD 공정 결과에 대한 종합적인 해석 및 평가를 요청한다.
웨이퍼 보트 위치별 박막 두께 특성 및 편차 원인 분석
두께 균일도가 상대적으로 취약한 위치 식별
공정 구조적 요인(가스 흐름, 반응물 도달성, 상·하 위치 효과)에 대한 해석
향후 공정 개선 또는 모니터링 포인트에 대한 제안`;

    return queryText;
}

async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            document.body.removeChild(textArea);
            return true;
        } catch (err) {
            document.body.removeChild(textArea);
            return false;
        }
    }
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
        
        const headerContent = document.createElement('div');
        headerContent.className = 'proc-header-content';
        headerContent.innerHTML = `
            <div class="proc-row">공정 ${proc.row_num}</div>
            <div class="proc-row"><span class="label">START :</span><span class="value">${proc.start_time}</span></div>
            <div class="proc-row"><span class="label">END :</span><span class="value">${proc.end_time}</span></div>
        `;
        
        const copyBtn = document.createElement('button');
        copyBtn.className = 'copy-llm-btn';
        copyBtn.textContent = 'LLM 질의문 복사';
        copyBtn.addEventListener('click', async () => {
            const queryText = generateLLMQuery(proc);
            const success = await copyToClipboard(queryText);
            if (success) {
                const originalText = copyBtn.textContent;
                copyBtn.textContent = '복사 완료!';
                copyBtn.style.background = '#4caf50';
                setTimeout(() => {
                    copyBtn.textContent = originalText;
                    copyBtn.style.background = '';
                }, 2000);
            } else {
                alert('복사에 실패했습니다. 브라우저가 클립보드 접근을 지원하지 않습니다.');
            }
        });
        
        header.appendChild(headerContent);
        header.appendChild(copyBtn);
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
    try {
        const res = await fetch('/api/trace_info?limit=300');
        if (!res.ok) {
            const errorText = await res.text();
            console.error('API 오류:', res.status, errorText);
            throw new Error(`서버 오류: ${res.status} ${errorText.substring(0, 50)}`);
        }
        const data = await res.json();
        data.sort((a, b) => new Date(a.start_time) - new Date(b.start_time)); // 오래된 -> 최신
        allData = data;
        filteredData = [...allData];
        calcLayout();
        pageIndex = 0; // 가장 오래된 공정부터 시작
        renderPage();
        fetchCurrentStepInfo();
    } catch (e) {
        console.error('데이터 로드 실패:', e);
        const container = document.getElementById('process-container');
        if (container) {
            container.innerHTML = `<div style="padding: 20px; text-align: center; color: #d32f2f;">
                <p>데이터를 불러올 수 없습니다.</p>
                <p style="font-size: 0.9em; color: #666;">${e.message}</p>
            </div>`;
        }
    }
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

function sortByNewest() {
    filteredData.sort((a, b) => new Date(b.start_time) - new Date(a.start_time)); // 최신 -> 과거
    pageIndex = 0;
    renderPage();
}

function sortByOldest() {
    filteredData.sort((a, b) => new Date(a.start_time) - new Date(b.start_time)); // 과거 -> 최신
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
    document.getElementById('sort-newest')?.addEventListener('click', sortByNewest);
    document.getElementById('sort-oldest')?.addEventListener('click', sortByOldest);
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
