let predictColumns = [];
let charts = new Map();
let logsByParameter = new Map();
let currentRequestId = 0;

const highlightPlugin = {
    id: 'highlightRegion',
    beforeDatasetsDraw(chart, args, opts) {
        const regions = opts?.regions || [];
        if (!regions.length) return;
        const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
        ctx.save();
        ctx.fillStyle = 'rgba(255, 193, 7, 0.2)';
        regions.forEach(region => {
            const xStart = x.getPixelForValue(region.start);
            const xEnd = x.getPixelForValue(region.end);
            ctx.fillRect(xStart, top, xEnd - xStart, bottom - top);
        });
        ctx.restore();
    }
};
Chart.register(highlightPlugin);

function safeId(text) {
    return text.replace(/[^a-zA-Z0-9]/g, '_');
}

function formatLocal(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function setStatus(message, type = 'info') {
    const statusEl = document.getElementById('status-message');
    if (!statusEl) return;
    statusEl.textContent = message || '';
    statusEl.style.color = type === 'error' ? '#c53030' : '#495057';
}

function getSelectedParameters() {
    const list = document.querySelectorAll('#parameter-list input[type="checkbox"]');
    const selected = [];
    list.forEach(cb => {
        if (cb.checked) selected.push(cb.value);
    });
    return selected;
}

function populateParameterList(columns) {
    const container = document.getElementById('parameter-list');
    if (!container) return;
    container.innerHTML = '';
    columns.forEach(col => {
        const option = document.createElement('label');
        option.className = 'param-option';
        const id = `param-${safeId(col)}`;
        option.innerHTML = `
            <input type="checkbox" id="${id}" value="${col}" checked>
            <span>${col}</span>
        `;
        container.appendChild(option);
    });
}

function setDefaultRange() {
    const endInput = document.getElementById('endTime');
    const startInput = document.getElementById('startTime');
    if (!endInput || !startInput) return;
    const now = new Date();
    const end = new Date(now.getTime() - now.getSeconds() * 1000);
    const start = new Date(end.getTime() - 60 * 60 * 1000);
    const toValue = (date) => {
        const pad = (n) => String(n).padStart(2, '0');
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
    };
    endInput.value = toValue(end);
    startInput.value = toValue(start);
}

function toTimestampString(value) {
    if (!value) return null;
    // datetime-local is in local timezone without seconds
    return value.replace('T', ' ') + ':00';
}

function parseTimestamp(value) {
    if (!value) return null;
    const iso = value.replace(' ', 'T');
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return null;
    return date;
}

function calcSegments(actual, predicted) {
    const predMap = new Map(predicted.map(d => [d.x, d.y]));
    const segments = [];
    const regions = [];
    let segStart = null;
    let lastTime = null;
    let maxDiff = 0;

    actual.forEach(point => {
        const pv = predMap.get(point.x);
        const time = new Date(point.x).getTime();
        if (Number.isNaN(time)) return;
        if (pv === undefined || pv === null) {
            if (segStart !== null && lastTime !== null && lastTime - segStart >= 10000) {
                regions.push({ start: segStart, end: lastTime });
                segments.push({ start: segStart, end: lastTime, max: maxDiff });
            }
            segStart = null;
            maxDiff = 0;
            lastTime = time;
            return;
        }
        const base = Math.abs(point.y) || 10;
        const diffPct = Math.abs(point.y - pv) / base * 100;
        if (diffPct > 10) {
            if (segStart === null) {
                segStart = time;
                maxDiff = diffPct;
            } else {
                maxDiff = Math.max(maxDiff, diffPct);
            }
        } else if (segStart !== null) {
            if (lastTime !== null && lastTime - segStart >= 10000) {
                regions.push({ start: segStart, end: lastTime });
                segments.push({ start: segStart, end: lastTime, max: maxDiff });
            }
            segStart = null;
            maxDiff = 0;
        }
        lastTime = time;
    });

    if (segStart !== null && lastTime !== null && lastTime - segStart >= 10000) {
        regions.push({ start: segStart, end: lastTime });
        segments.push({ start: segStart, end: lastTime, max: maxDiff });
    }

    return { segments, regions };
}

function createChartCard(parameter) {
    const container = document.getElementById('charts-container');
    const card = document.createElement('div');
    card.className = 'chart-card';
    const safe = safeId(parameter);
    card.innerHTML = `
        <div class="chart-header">
            <div class="chart-title">${parameter}</div>
        </div>
        <canvas id="chart-${safe}"></canvas>
        <div class="chart-message" id="message-${safe}"></div
    `;
    container.appendChild(card);
    return {
        card,
        canvas: card.querySelector('canvas'),
        messageEl: card.querySelector('.chart-message')
    };
}

function renderChart(parameter, actual, predicted, regions, logs) {
    const safe = safeId(parameter);
    const canvas = document.getElementById(`chart-${safe}`);
    if (!canvas) return;
    if (charts.has(parameter)) {
        charts.get(parameter).destroy();
        charts.delete(parameter);
    }

    const yValues = actual.concat(predicted).map(d => d.y).filter(v => v !== null && v !== undefined);
    if (!yValues.length) return;
    const yMin = Math.min(...yValues);
    const yMax = Math.max(...yValues);

    const logLines = [];
    logs.forEach(log => {
        const iso = log.timestamp ? String(log.timestamp).replace(' ', 'T') : null;
        if (!iso) return;
        logLines.push({ x: iso, y: yMin });
        logLines.push({ x: iso, y: yMax });
        logLines.push({ x: null, y: null });
    });

    const chart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            datasets: [
                {
                    label: '예측값',
                    data: predicted.map(d => ({ x: d.x, y: d.y })),
                    borderColor: '#ef5350',
                    tension: 0.3,
                    pointRadius: 0,
                },
                {
                    label: '실제값',
                    data: actual.map(d => ({ x: d.x, y: d.y })),
                    borderColor: '#1e88e5',
                    tension: 0.3,
                    pointRadius: 0,
                },
                {
                    label: '이상 로그',
                    data: logLines,
                    borderColor: '#ff9800',
                    borderWidth: 1,
                    borderDash: [6, 6],
                    pointRadius: 0,
                    spanGaps: false,
                    showLine: true,
                }
            ]
        },
        options: {
            animation: false,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: { usePointStyle: true }
                },
                highlightRegion: { regions }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { tooltipFormat: 'yyyy-MM-dd HH:mm:ss' }
                },
                y: {
                    min: yMin,
                    max: yMax,
                    ticks: { autoSkip: true }
                }
            }
        }
    });

    charts.set(parameter, chart);
}

function destroyCharts() {
    charts.forEach(chart => chart.destroy());
    charts.clear();
}

function renderLogList(logs) {
    const container = document.getElementById('log-list');
    if (!container) return;
    container.innerHTML = '';
    if (!logs.length) {
        const empty = document.createElement('div');
        empty.textContent = '선택한 기간에 해당하는 이상 로그가 없습니다.';
        empty.style.color = '#666';
        container.appendChild(empty);
        return;
    }
    logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    logs.forEach(log => {
        const entry = document.createElement('div');
        entry.className = 'log-entry';
        entry.innerHTML = `
            <div class="log-time">${formatLocal(log.timestamp)}</div>
            <div class="log-parameter">${log.parameter}</div>
            <div class="log-message">${log.message}</div>
        `;
        container.appendChild(entry);
    });
}

async function fetchLogs(start, end) {
    logsByParameter = new Map();
    try {
        const res = await fetch(`/api/history/logs?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`);
        if (!res.ok) throw new Error('로그 조회 실패');
        const data = await res.json();
        data.forEach(item => {
            const key = item.parameter;
            if (!logsByParameter.has(key)) logsByParameter.set(key, []);
            logsByParameter.get(key).push(item);
        });
        renderLogList(data);
    } catch (err) {
        renderLogList([]);
        console.error(err);
        setStatus('로그를 불러오지 못했습니다. 입력한 기간과 서버 상태를 확인하세요.', 'error');
    }
}

async function loadHistory() {
    const startValue = toTimestampString(document.getElementById('startTime')?.value);
    const endValue = toTimestampString(document.getElementById('endTime')?.value);
    if (!startValue || !endValue) {
        setStatus('시작 시간과 종료 시간을 모두 입력해주세요.', 'error');
        return;
    }
    const startDate = parseTimestamp(startValue);
    const endDate = parseTimestamp(endValue);
    if (!startDate || !endDate) {
        setStatus('시간 형식이 올바르지 않습니다.', 'error');
        return;
    }
    if (startDate > endDate) {
        setStatus('시작 시간이 종료 시간보다 늦을 수 없습니다.', 'error');
        return;
    }

    const params = getSelectedParameters();
    if (!params.length) {
        setStatus('최소 한 개의 파라미터를 선택해주세요.', 'error');
        return;
    }

    const requestId = ++currentRequestId;
    setStatus('데이터를 불러오는 중입니다...');
    destroyCharts();
    const chartsContainer = document.getElementById('charts-container');
    chartsContainer.innerHTML = '';

    await fetchLogs(startValue, endValue);
    if (requestId !== currentRequestId) return;

    for (const param of params) {
        const { messageEl } = createChartCard(param);
        const chartLogs = logsByParameter.get(param) || [];
        try {
            const res = await fetch(`/api/trace_pred_chart?param=${encodeURIComponent(param)}&start=${encodeURIComponent(startValue)}&end=${encodeURIComponent(endValue)}`);
            if (!res.ok) throw new Error('데이터 조회 실패');
            const json = await res.json();
            if (requestId !== currentRequestId) return;

            const actual = json.actual || [];
            const predicted = json.predicted || [];
            if (!actual.length && !predicted.length) {
                if (messageEl) {
                    messageEl.textContent = '표시할 데이터가 없습니다.';
                }
                continue;
            }
            if (messageEl) messageEl.textContent = '';

            const { regions } = calcSegments(actual, predicted);
            renderChart(param, actual, predicted, regions, chartLogs);
        } catch (err) {
            console.error(err);
            if (messageEl) {
                messageEl.textContent = '데이터를 불러오는데 실패했습니다.';
            }
        }
    }

    setStatus('조회가 완료되었습니다.');
}

async function initPage() {
    setDefaultRange();
    try {
        const res = await fetch('/api/model_columns');
        if (!res.ok) throw new Error('모델 컬럼 조회 실패');
        predictColumns = await res.json();
        populateParameterList(predictColumns);
    } catch (err) {
        console.error(err);
        setStatus('예측 컬럼 정보를 불러오지 못했습니다.', 'error');
    }

    document.getElementById('selectAll')?.addEventListener('click', () => {
        document.querySelectorAll('#parameter-list input[type="checkbox"]').forEach(cb => cb.checked = true);
    });
    document.getElementById('clearAll')?.addEventListener('click', () => {
        document.querySelectorAll('#parameter-list input[type="checkbox"]').forEach(cb => cb.checked = false);
    }); document.getElementById('searchBtn')?.addEventListener('click', () => {
        loadHistory();
    });

    // 초기 로드
    loadHistory();
}

window.addEventListener('DOMContentLoaded', initPage);