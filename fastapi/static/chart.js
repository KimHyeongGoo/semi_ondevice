const charts = {};
const columns = Array.from(document.querySelectorAll(".toggle-chart")).map(cb => (cb.dataset.col || "").trim());
const columnLookup = new Set(columns);
const hasColumn = (name) => columnLookup.has((name || "").trim());
const logEntryByParam = new Map();
const logEntryByKey = new Map();
const normalizeParam = (name) => (name || "").trim();
const buildLogKey = (log) => {
    const msg = typeof log.message === 'string' ? log.message : JSON.stringify(log.message || {});
    return `${normalizeParam(log.parameter)}||${log.timestamp || ''}||${msg || ''}`;
};

let highlightState = { param: null, key: null };
let settingsCache = {};
const chartBoxByParam = new Map();

function scrollElementIntoCenter(el) {
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function scrollChartIntoView(param) {
    const target = chartBoxByParam.get(param) || document.querySelector(`.chart-box[data-param="${param}"]`);
    scrollElementIntoCenter(target);
}

function scrollLogIntoView(param, key) {
    let target = null;
    if (key && logEntryByKey.has(key)) {
        target = logEntryByKey.get(key);
    }
    if (!target) {
        const entries = logEntryByParam.get(param) || [];
        entries.forEach(el => {
            if (!target || el.offsetTop < target.offsetTop) target = el;
        });
    }
    scrollElementIntoCenter(target);
}

function applyHighlight() {
    const { param, key } = highlightState;
    document.querySelectorAll('.chart-box').forEach(box => {
        const match = param && normalizeParam(box.dataset.param) === param;
        box.classList.toggle('highlight', Boolean(param) && match);
    });

    logEntryByParam.forEach((entries, p) => {
        const matchParam = param && p === param;
        entries.forEach(el => {
            const entryKey = el.dataset.key;
            const active = Boolean(param) && (key ? entryKey === key : matchParam);
            el.classList.toggle('highlight', active);
        });
    });
}

function setHighlight(newState, options = {}) {
    highlightState = newState;
    applyHighlight();
    const active = Boolean(highlightState.param);
    if (active && options.scrollToChart) {
        scrollChartIntoView(highlightState.param);
    }
    if (active && options.scrollToLog) {
        scrollLogIntoView(highlightState.param, highlightState.key);
    }
}

function toggleHighlightForParam(param) {
    const target = param ? normalizeParam(param) : null;
    if (!target) {
        setHighlight({ param: null, key: null });
        return;
    }
    const isActive = highlightState.param === target && !highlightState.key;
    if (isActive) {
        setHighlight({ param: null, key: null });
    } else {
        setHighlight({ param: target, key: null }, { scrollToLog: true });
    }
}

function toggleHighlightForEntry(param, key) {
    const target = param ? normalizeParam(param) : null;
    if (!target || !key) {
        setHighlight({ param: null, key: null });
        return;
    }
    const isActive = highlightState.key === key;
    if (isActive) {
        setHighlight({ param: null, key: null });
    } else {
        setHighlight({ param: target, key }, { scrollToChart: true });
    }
}

function parseLogPayload(raw) {
    if (!raw) return {};
    if (typeof raw === 'object') return raw;
    try {
        return JSON.parse(raw);
    } catch (e) {
        try {
            const fixed = raw.replace(/'/g, '"');
            return JSON.parse(fixed);
        } catch (e2) {
            return {};
        }
    }
}

function formatPayloadTime(val, fallback) {
    if (!val) return fallback;
    const d = new Date(val);
    if (!isNaN(d)) return formatKoreanTime(d);
    return val;
}

function formatLimitType(raw) {
    if (!raw) return '이상';
    const t = String(raw).toLowerCase();
    if (t.includes('min') || t.includes('하한')) return '하한선';
    if (t.includes('max') || t.includes('상한')) return '상한선';
    return raw;
}
let selectedDuration = 300; // 기본값: 5분
let selectedStep = 10; //
let hiddenColumns = [];
let limits = {};
let lastStepFallbackFetch = 0;
let modalChart = null; // ECharts instance
let modalChartFallback = null; // Chart.js fallback
let modalOpenCol = null;

const categoryMap = {
    MFC: ["MFC7_DCS", "MFC8_NH3", "MFC1_N2-1", "MFC2_N2-2", "MFC3_N2-3", "MFC4_N2-4"],
    Pressure: ["VG11 Press value", "VG12 Press value", "VG13 Press value"],
    Temperature: ["Temp_Act_U", "Temp_Act_CU", "Temp_Act_C", "Temp_Act_CL", "Temp_Act_L"],
    Actuator: ["MFC26_F.PWR", "MFC27_L.POS", "MFC28_R.POS"]
};
const stepNames = {
    2: 'END', 0: 'STANDBY/IDLE', 1: 'START', 17: 'B.UP', 3: 'WAIT',
    74: 'S.P-1', 75: 'S.P-2', 25: 'R.UP1', 22: 'STAB1', 76: 'S.P-3',
    81: 'M.P-3', 72: 'L.CHK', 44: 'PREPRG1', 99: 'EVAC1', 100: 'EVAC2',
    111: 'N-EVA1', 128: 'CLOSE1', 119: 'SI-FL1', 117: 'SI-EVA1', 152: 'CHANGE',
    113: 'N-PRE1', 115: 'N-FL1', 116: 'N-FL2', 110: 'pre-NH3P', 49: 'DEPO1',
    135: 'post_NH3P', 103: 'N2PRG1', 149: 'SI-EVA4', 85: 'A.VAC2', 90: 'A.PRG2',
    84: 'A.VAC1', 89: 'A.PRG1', 104: 'N2PRG2', 105: 'N2PRG3', 86: 'A.VAC3',
    91: 'A.PRG3', 87: 'A.VAC4', 92: 'A.PRG4', 130: 'CYCLE1', 93: 'A.PRG5',
    31: 'R.DOWN1', 94: 'B.FILL1', 95: 'B.FILL2', 96: 'B.FILL3', 97: 'B.FILL4',
    98: 'B.FILL5', 18: 'B.DOWN'
};

function parseTimeString(tstr) {
    return new Date(tstr.replace(" ", "T").replace(/\.\d+$/, ''));
}

async function loadSettings() {
    try {
        const res = await fetch('/api/settings');
        if (!res.ok) return;
        const s = await res.json();
        settingsCache = s || {};
        if (s.duration) selectedDuration = s.duration;
        if (s.step) selectedStep = s.step;
        hiddenColumns = s.hidden_columns || [];
        document.getElementById('duration-select').value = String(selectedDuration);
        document.getElementById('step-select').value = String(selectedStep);
        document.querySelectorAll('.toggle-chart').forEach(cb => {
            const col = cb.dataset.col;
            cb.checked = !hiddenColumns.includes(col);
            const canvas = document.querySelector(`#chart-${col}`);
            if (canvas) canvas.style.display = cb.checked ? 'block' : 'none';
            const box = document.getElementById(`chart-box-${col}`);
            if (box) box.style.order = cb.checked ? box.dataset.order : 999;
            const text = cb.closest('.toggle-container').querySelector('.toggle-text');
            if (text) text.textContent = cb.checked ? '숨기기' : col;
        });
    } catch (e) {
        console.error('failed to load settings', e);
    }
}

async function saveSettings() {
    const hidden = [];
    document.querySelectorAll('.toggle-chart').forEach(cb => {
        if (!cb.checked) hidden.push(cb.dataset.col);
    });
    const body = { ...settingsCache, duration: selectedDuration, step: selectedStep, hidden_columns: hidden };
    try {
        await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        settingsCache = body;
    } catch (e) {
        console.error('failed to save settings', e);
    }
}

function createCharts() {
    columns.forEach((col, idx) => {
        const ctx = document.getElementById(`chart-${col}`).getContext("2d");
        const isLast = idx == columns.length - 1;
        charts[col] = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [
                    { label: "Actual", borderColor: "blue", data: [], tension: 0.3 },
                    { label: "Predicted", borderColor: "red", data: [], tension: 0.3 },
                    { label: "Upper Limit", borderColor: "green", borderDash: [5, 5], data: [], pointRadius: 0 },
                    { label: "Lower Limit", borderColor: "orange", borderDash: [5, 5], data: [], pointRadius: 0 }
                ]
            },
            options: {
                plugins: {
                    title: {
                        display: true,
                        text: col,
                        position: 'left',
                        padding: { top: 8, bottom: 4 },
                        font: { size: 14, weight: 'bold' }
                    },
                    legend: { display: true, position: 'top' },
                    zoom: undefined
                },
                animation: false,
                parsing: false,
                responsive: true,
                maintainAspectRatio: false,  // HTML height를 따르도록
                scales: {
                    x: {
                        type: 'time',
                        display: true,
                        ticks: { display: true },
                        title: { display: true, text: 'Time' },
                        grid: { display: false }
                    },
                    y: { title: { display: true, text: "Value" } }
                }
            }
        });
    });
}

async function fetchAndUpdate() {
    const res = await fetch(`/api/data?duration=${selectedDuration}&step=${selectedStep}`);
    const json = await res.json();
    limits = json.limits || {};

    columns.forEach(col => {
        const chart = charts[col];
        const actual = json[col].actual;
        const predicted = json[col].predicted;

        const all = actual.concat(predicted);
        const values = all.map(d => d.value);
        const times = all.map(d => parseTimeString(d.time));

        let yMin, yMax;
        if (col.startsWith("Temp_Act")) {
            yMin = Math.min(...values) - 100;
            yMax = Math.max(...values) + 100;
        } else if (col.startsWith("VG11")) {
            yMin = Math.min(...values) - 50;
            yMax = Math.max(...values) + 50;
        } else {
            yMin = Math.min(...values) - 3;
            yMax = Math.max(...values) + 3;
        }


        const xMin = new Date(Math.min(...times));
        const xMax = new Date(Math.max(...times));

        chart.data.datasets[0].data = actual.map(d => ({ x: parseTimeString(d.time), y: d.value }));
        chart.data.datasets[1].data = predicted.map(d => ({ x: parseTimeString(d.time), y: d.value }));

        const upper = [], lower = [];
        for (let d of predicted) {
            const t = parseTimeString(d.time);
            const step = d.step_id?.toString();
            const limit = limits?.[col]?.[step] || limits?.[col]?.["all"];
            if (limit) {
                upper.push({ x: t, y: limit.max });
                lower.push({ x: t, y: limit.min });
            }
        }

        chart.data.datasets[2].data = upper;
        chart.data.datasets[3].data = lower;

        chart.options.scales.y.min = yMin;
        chart.options.scales.y.max = yMax;
        chart.options.scales.x.min = xMin;
        chart.options.scales.x.max = xMax;
        chart.update();


        const cb = document.querySelector(`.toggle-chart[data-col="${col}"]`);
        const canvas = document.getElementById(`chart-${col}`);
        if (cb && canvas) {
            canvas.style.display = cb.checked ? 'block' : 'none';
        }
    });

    const latestStep = extractLatestStep(json);
    if (latestStep) {
        updateCurrentStepDisplay(latestStep.step_id, latestStep.step_name);
    } else {
        fetchCurrentStepFallback();
    }
}

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

function extractLatestStep(data) {
    let latestEntry = null;
    let latestTime = 0;
    for (const col of columns) {
        const actual = data[col]?.actual || [];
        for (let i = actual.length - 1; i >= 0; i--) {
            const entry = actual[i];
            if (!entry) continue;
            const hasStepInfo = (entry.step_id !== null && entry.step_id !== undefined) || (entry.step_name && entry.step_name !== '');
            if (!hasStepInfo) continue;
            const parsed = parseTimeString(entry.time);
            const timeValue = parsed ? parsed.getTime() : 0;
            if (!latestEntry || timeValue > latestTime) {
                latestEntry = entry;
                latestTime = timeValue;
            }
            break;
        }
    }
    if (!latestEntry) return null;
    return {
        step_id: latestEntry.step_id ?? null,
        step_name: latestEntry.step_name ?? null
    };
}

async function fetchCurrentStepFallback() {
    const now = Date.now();
    if (now - lastStepFallbackFetch < 5000) return;
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

function createSettingsUI() {
    const catWrap = document.getElementById("category-buttons");
    const paramWrap = document.getElementById("param-buttons");
    const form = document.getElementById("settings-form");
    catWrap.innerHTML = '';
    paramWrap.innerHTML = '';
    form.innerHTML = '';

    const categories = Object.keys(categoryMap).filter(cat => categoryMap[cat].some(hasColumn));
    if (categories.length === 0) return;
    let activeCat = categories[0];
    let activeParam = categoryMap[activeCat].find(hasColumn) || columns[0];

    function renderCategories() {
        catWrap.innerHTML = '';
        categories.forEach(cat => {
            const btn = document.createElement("button");
            btn.className = "category-btn" + (cat === activeCat ? " active" : "");
            btn.textContent = cat;
            btn.onclick = () => {
                activeCat = cat;
                activeParam = categoryMap[cat].find(hasColumn) || activeParam;
                renderCategories();
                renderParams();
                renderStepTable(activeParam);
            };
            catWrap.appendChild(btn);
        });
    }

    function renderParams() {
        paramWrap.innerHTML = '';
        categoryMap[activeCat].forEach(col => {
            if (!hasColumn(col)) return;
            const btn = document.createElement("button");
            btn.className = "param-btn" + (col === activeParam ? " active" : "");
            btn.textContent = col;
            btn.onclick = () => {
                activeParam = col;
                renderParams();
                renderStepTable(col);
            };
            paramWrap.appendChild(btn);
        });
    }

    renderCategories();
    renderParams();
    renderStepTable(activeParam);
}

function renderStepTable(col) {
    const form = document.getElementById("settings-form");
    form.innerHTML = '';  // 기존 테이블 제거

    const stepIds = Object.keys(stepNames).map(Number).sort((a, b) => a - b);
    const table = document.createElement("table");
    table.className = "step-table";

    // ⬆️ 1. 테이블 헤더 생성
    const thead = document.createElement("thead");
    thead.innerHTML = `<tr><th>Step ID</th><th>Step Name</th><th>Min</th><th>Max</th></tr>`;
    table.appendChild(thead);

    // ⬇️ 2. 테이블 바디 생성
    const tbody = document.createElement("tbody");

    // ✅ (1) "All" 공통 설정 행
    const commonLim = limits?.[col]?.["all"] || {};
    const commonRow = document.createElement("tr");
    commonRow.innerHTML = `
        <td><strong>All</strong></td>
        <td><em>모든 Step 공통</em></td>
        <td><input data-col="${col}" data-step="all" data-type="min" value="${commonLim.min ?? ''}" /></td>
        <td><input data-col="${col}" data-step="all" data-type="max" value="${commonLim.max ?? ''}" /></td>
    `;
    tbody.appendChild(commonRow);

    // ✅ (2) Step ID별 행 추가
    stepIds.forEach(id => {
        const stepKey = id.toString();
        const lim = limits?.[col]?.[stepKey] || {};
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${id}</td>
            <td>${stepNames[id] || "UNKNOWN"}</td>
            <td><input data-col="${col}" data-step="${stepKey}" data-type="min" value="${lim.min ?? ''}" /></td>
            <td><input data-col="${col}" data-step="${stepKey}" data-type="max" value="${lim.max ?? ''}" /></td>
        `;
        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    form.appendChild(table);
}


function collectLimits() {
    const inputs = document.querySelectorAll("#settings-form input");
    const newLimits = {};
    inputs.forEach(inp => {
        const col = inp.dataset.col;
        const step = inp.dataset.step;
        const typ = inp.dataset.type;
        const val = parseFloat(inp.value);
        if (!newLimits[col]) newLimits[col] = {};
        if (!newLimits[col][step]) newLimits[col][step] = {};
        if (!isNaN(val)) newLimits[col][step][typ] = val;
    });
    return newLimits;
}

async function saveLimits() {
    const updatedPart = collectLimits();  // 현재 탭에서 입력한 칼럼의 일부만 들어있음

    // 기존 limits와 병합
    const merged = { ...limits };
    Object.entries(updatedPart).forEach(([col, steps]) => {
        if (!merged[col]) merged[col] = {};
        Object.entries(steps).forEach(([step, val]) => {
            merged[col][step] = val;
        });
    });

    const res = await fetch("/api/save_limits", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(merged)
    });

    if (res.ok) {
        limits = merged;
        document.getElementById("settings-modal").style.display = "none";
        fetchAndUpdate();
    } else {
        alert("저장 실패");
    }
}

function openChartModal(col) {
    const modal = document.getElementById('chart-modal');
    const modalTitle = document.getElementById('modal-title');
    const container = document.getElementById('modal-echart');
    const fallbackCanvas = document.getElementById('modal-canvas');
    if (!modal || !modalTitle || !container || !fallbackCanvas || !charts[col]) return;
    const datasets = cloneChartData(charts[col]).datasets;
    renderModalChart(col, datasets, true);
}

function closeChartModal() {
    const modal = document.getElementById('chart-modal');
    if (modalChart) {
        modalChart.clear();
        modalChart = null;
    }
    if (modalChartFallback) {
        modalChartFallback.destroy();
        modalChartFallback = null;
    }
    modalOpenCol = null;
    if (modal) modal.style.display = 'none';
}

function cloneChartData(srcChart) {
    const clonedDatasets = srcChart.data.datasets.map(ds => ({
        ...ds,
        data: ds.data.map(pt => {
            if (pt && typeof pt === 'object') {
                return {
                    x: pt.x ? new Date(pt.x) : pt.x,
                    y: pt.y
                };
            }
            return pt;
        })
    }));
    return { datasets: clonedDatasets };
}

function renderModalChart(param, datasets, showModal = false) {
    const modal = document.getElementById('chart-modal');
    const modalTitle = document.getElementById('modal-title');
    const container = document.getElementById('modal-echart');
    const fallbackCanvas = document.getElementById('modal-canvas');
    if (!modal || !modalTitle || !container || !fallbackCanvas) return;

    const regions = []; // no special regions; upper/lower는 별도 라인으로 존재
    const useEcharts = typeof echarts !== 'undefined';
    container.style.display = useEcharts ? 'block' : 'none';
    fallbackCanvas.style.display = useEcharts ? 'none' : 'block';

    if (useEcharts) {
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
                lineStyle: { width: ds.borderWidth || 2, color: ds.borderColor || undefined },
                itemStyle: { color: ds.borderColor || undefined }
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
                    legend: { display: true }
                },
                scales: { x: { type: 'time' }, y: { type: 'linear' } }
            }
        });
    }

    modalOpenCol = param;
    modalTitle.textContent = param;
    if (showModal) modal.style.display = 'flex';
    setTimeout(() => {
        if (modalChart) modalChart.resize();
        if (modalChartFallback) modalChartFallback.resize();
    }, 0);
}

async function fetchLogs() {
    try {
        const res = await fetch('/api/logs');
        const logs = await res.json();
        const logContent = document.getElementById('log-content');

        logEntryByParam.clear();
        logEntryByKey.clear();
        if (!logs || logs.length === 0) {
            logContent.innerText = "(최근 이벤트 없음)";
        } else {
            logContent.innerHTML = '';
            logs.forEach(log => {
                const ts = formatKoreanTime(log.timestamp);
                const param = normalizeParam(log.parameter);
                const key = buildLogKey(log);
                const payload = parseLogPayload(log.message);
                const violationTime = formatPayloadTime(payload['시간'] || payload.time || payload.timestamp || payload.end || payload.start, ts);
                const limitTypeRaw = payload['이상종류'] || payload.limit_type || payload.type || '이상';
                const limitType = formatLimitType(limitTypeRaw);
                const predictedVal = payload['예측값'] ?? payload.predicted_value ?? payload.predicted ?? payload.actual_value;
                const thresholdVal = payload['임계값'] ?? payload.threshold ?? payload.max ?? payload.min;
                const predText = typeof predictedVal === 'number' ? predictedVal.toFixed(3) : (predictedVal ?? '-');
                const thrText = typeof thresholdVal === 'number' ? thresholdVal : (thresholdVal ?? '-');
                const summary = `[${param}] ${limitType} 침범 예상`;
                const detail = `예측값: ${predText}, 임계값: ${thrText}`;
                const entry = document.createElement('div');
                entry.className = 'timeline-entry';
                entry.dataset.param = param;
                entry.dataset.key = key;
                entry.innerHTML = `
                    <div class="timeline-icon">⚠</div>
                    <div class="timeline-body">
                        <div class="timeline-time">${ts}</div>
                        <div class="timeline-text"><strong>${summary}</strong><br>${detail}</div>
                    </div>
                `;
                entry.addEventListener('click', () => toggleHighlightForEntry(param, key));
                logContent.appendChild(entry);
                if (!logEntryByParam.has(param)) logEntryByParam.set(param, []);
                logEntryByParam.get(param).push(entry);
                logEntryByKey.set(key, entry);
            });
            if (highlightState.key && !logEntryByKey.has(highlightState.key)) {
                setHighlight({ param: null, key: null });
            } else {
                applyHighlight();
            }
        }
    } catch (e) {
        console.error("로그 로딩 실패:", e);
        const logContent = document.getElementById('log-content');
        logContent.innerText = "(로그 불러오기 오류)";
    }
}

function formatKoreanTime(ts) {
    const d = new Date(ts);
    if (isNaN(d)) return ts;
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}년 ${pad(d.getMonth() + 1)}월 ${pad(d.getDate())}일 ${pad(d.getHours())}시 ${pad(d.getMinutes())}분 ${pad(d.getSeconds())}초`;
}


// 초기 실행
window.addEventListener("DOMContentLoaded", async () => {
    document.querySelectorAll('.chart-box').forEach(box => {
        box.style.order = box.dataset.order;
        const param = normalizeParam(box.dataset.param);
        if (param) chartBoxByParam.set(param, box);
    });
    await loadSettings();
    createCharts();
    fetchAndUpdate();
    fetchCurrentStepFallback();
    setInterval(fetchAndUpdate, 1000);
    fetchLogs();
    setInterval(fetchLogs, 1000);

    document.getElementById("duration-select").addEventListener("change", e => {
        selectedDuration = parseInt(e.target.value);
        fetchAndUpdate();
        saveSettings();
    });

    document.getElementById("step-select").addEventListener("change", e => {
        selectedStep = parseInt(e.target.value);
        fetchAndUpdate();
        saveSettings();
    });


    document.querySelectorAll(".toggle-chart").forEach(cb => {
        cb.addEventListener("change", () => {
            const col = cb.dataset.col;
            const canvas = document.querySelector(`#chart-${col}`);
            if (canvas) canvas.style.display = cb.checked ? "block" : "none";
            const box = document.getElementById(`chart-box-${col}`);
            if (box) box.style.order = cb.checked ? box.dataset.order : 999;
            const text = cb.closest('.toggle-container').querySelector('.toggle-text');
            if (text) text.textContent = cb.checked ? '숨기기' : col;
            saveSettings();
        });
    });

    document.getElementById("open-settings").addEventListener("click", () => {
        createSettingsUI();
        document.getElementById("settings-modal").style.display = "block";
    });

    document.getElementById("save-settings").addEventListener("click", saveLimits);

    document.querySelectorAll(".expand-btn").forEach(btn => {
        btn.addEventListener("click", () => openChartModal(btn.dataset.col));
    });

    const modal = document.getElementById('chart-modal');
    const closeBtn = document.getElementById('close-modal');
    if (closeBtn) closeBtn.addEventListener('click', closeChartModal);
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) closeChartModal();
        });
    }

    // 차트 박스 클릭 시 해당 파라미터 하이라이트 토글
    document.querySelectorAll(".chart-box").forEach(box => {
        box.addEventListener("click", (e) => {
            if (e.target.closest('.expand-btn') || e.target.closest('.toggle-container') || e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON') return;
            const param = box.dataset.param ? normalizeParam(box.dataset.param) : null;
            if (param) toggleHighlightForParam(param);
        });
    });
});

window.addEventListener('resize', () => {
    if (modalChart) modalChart.resize();
    if (modalChartFallback) modalChartFallback.resize();
});
