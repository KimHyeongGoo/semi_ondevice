const charts = {};
const columns = Array.from(document.querySelectorAll(".toggle-chart")).map(cb => cb.dataset.col);
let selectedDuration = 300; // 기본값: 5분
let selectedStep = 10; //
let hiddenColumns = [];
let limits = {};
let lastStepFallbackFetch = 0;
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
    const body = { duration: selectedDuration, step: selectedStep, hidden_columns: hidden };
    try {
        await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
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
                    legend: { display: true, position: 'top' }
                },
                animation: false,
                parsing: false,
                responsive: true,
                maintainAspectRatio: false,  // HTML height를 따르도록
                scales: {
                    // x: { type: "time", time: { tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss' } }, title: { display: true, text: "Time" } },
                    x: {
                        type: 'time',
                        display: isLast,                // 축 전체 표시 여부
                        ticks: { display: isLast },     // 눈금 레이블 표시 여부
                        title: { display: isLast, text: 'Time' },
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
    const tabContainer = document.getElementById("tab-container");
    const form = document.getElementById("settings-form");
    tabContainer.innerHTML = '';
    form.innerHTML = '';

    columns.forEach((col, idx) => {
        const tab = document.createElement("div");
        tab.className = "tab" + (idx === 0 ? " active" : "");
        tab.textContent = col;
        tab.onclick = () => {
            document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
            tab.classList.add("active");
            renderStepTable(col);
        };
        tabContainer.appendChild(tab);
    });
    renderStepTable(columns[0]);
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

async function fetchLogs() {
    try {
        const res = await fetch('/api/logs');
        const logs = await res.json();
        const logContent = document.getElementById('log-content');

        if (!logs || logs.length === 0) {
            logContent.innerText = "(최근 이벤트 없음)";
        } else {
            logContent.innerHTML = logs.map(log => {
                const encoded = encodeURIComponent(JSON.stringify({
                    time: log.timestamp,
                    parameter: log.parameter
                }));
                return `${log.message}<br><a href="/logview.html?info=${encoded}" target="_blank">[리포트 확인]</a>`;
            }).join("<br><br>");
        }
    } catch (e) {
        console.error("로그 로딩 실패:", e);
        const logContent = document.getElementById('log-content');
        logContent.innerText = "(로그 불러오기 오류)";
    }
}


// 초기 실행
window.addEventListener("DOMContentLoaded", async () => {
    document.querySelectorAll('.chart-box').forEach(box => {
        box.style.order = box.dataset.order;
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
});
