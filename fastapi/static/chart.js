const charts = {};
const columns = Array.from(document.querySelectorAll(".toggle-chart")).map(cb => cb.dataset.col);
let selectedDuration = 300; // 기본값: 5분
let limits = {};
const stepNames = {
    2: 'END', 0: 'STANDBY', 1: 'START', 17: 'B.UP', 3: 'WAIT',
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

function createCharts() {
    columns.forEach(col => {
        const ctx = document.getElementById(`chart-${col}`).getContext("2d");
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
                animation: false,
                parsing: false,
                scales: {
                    x: { type: "time", time: { tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss' } }, title: { display: true, text: "Time" } },
                    y: { title: { display: true, text: "Value" } }
                }
            }
        });
    });
}

async function fetchAndUpdate() {
    const res = await fetch(`/api/data?duration=${selectedDuration}`);
    const json = await res.json();
    limits = json.limits || {};

    columns.forEach(col => {
        const chart = charts[col];
        const actual = json[col].actual;
        const predicted = json[col].predicted;

        const all = actual.concat(predicted);
        const values = all.map(d => d.value);
        const times = all.map(d => parseTimeString(d.time));

        const yMin = Math.min(...values) - 3;
        const yMax = Math.max(...values) + 3;
        const xMin = new Date(Math.min(...times));
        const xMax = new Date(Math.max(...times));

        chart.data.datasets[0].data = actual.map(d => ({ x: parseTimeString(d.time), y: d.value }));
        chart.data.datasets[1].data = predicted.map(d => ({ x: parseTimeString(d.time), y: d.value }));

        const upper = [], lower = [];
        for (let d of predicted) {
            const t = parseTimeString(d.time);
            const step = d.step_id?.toString();
            const limit = limits?.[col]?.[step];
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
    });
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
    form.innerHTML = '';

    const stepIds = Object.keys(stepNames).map(Number).sort((a, b) => a - b);
    const table = document.createElement("table");
    table.className = "step-table";
    table.innerHTML = `<tr><th>Step ID</th><th>Step Name</th><th>Min</th><th>Max</th></tr>`;

    stepIds.forEach(id => {
        const tr = document.createElement("tr");
        const stepKey = id.toString();
        const lim = limits?.[col]?.[stepKey] || {};
        tr.innerHTML = `
            <td>${id}</td>
            <td>${stepNames[id] || "Unknown"}</td>
            <td><input data-col="${col}" data-step="${stepKey}" data-type="min" value="${lim.min ?? ''}" /></td>
            <td><input data-col="${col}" data-step="${stepKey}" data-type="max" value="${lim.max ?? ''}" /></td>
        `;
        table.appendChild(tr);
    });
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
            // 오래된 로그가 위로 가도록 reverse()
            logContent.innerText = logs.reverse().join("\n\n");
        }
    } catch (e) {
        console.error("로그 로딩 실패:", e);
        const logContent = document.getElementById('log-content');
        logContent.innerText = "(로그 불러오기 오류)";
    }
}

// 초기 실행
window.addEventListener("DOMContentLoaded", () => {
    createCharts();
    fetchAndUpdate();
    setInterval(fetchAndUpdate, 1000);
    fetchLogs();
    setInterval(fetchLogs, 1000);

    document.getElementById("duration-select").addEventListener("change", e => {
        selectedDuration = parseInt(e.target.value);
        fetchAndUpdate();
    });

    document.querySelectorAll(".toggle-chart").forEach(cb => {
        cb.addEventListener("change", () => {
            const col = cb.dataset.col;
            const box = document.getElementById(`chart-box-${col}`);
            box.style.display = cb.checked ? "block" : "none";
        });
    });

    document.getElementById("open-settings").addEventListener("click", () => {
        createSettingsUI();
        document.getElementById("settings-modal").style.display = "block";
    });

    document.getElementById("save-settings").addEventListener("click", saveLimits);
});
