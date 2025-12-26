const mainCharts = {};
const chartBoxesMap = {};
let selectedTimeRange = 5; // 기본값 5분 (분 단위)
let chartHoldEnabled = false; // 차트 업데이트 중지 여부
const logEntryElements = new Map();
let processStart = null;
let processEnd = null;
// logs structure: { param: [ {start, end, diff, actual_value, predicted_value, peak_time, step_id, step_name} ] }
let logs = {};
const loggedIds = new Set();
let lastServerLogEnd = 0;
let lastSeenLogEnd = 0;
let currentHighlight = null;
let warningModalOpen = false;
let modalChart = null; // ECharts instance for modal
let modalChartFallback = null; // Chart.js fallback when ECharts unavailable
let modalInfo = null;
let modalFrozen = false;
const warningToggleKey = 'anomalyWarningEnabled';
let warningEnabled = true;
let settingsCache = {};
const storedWarning = typeof localStorage !== 'undefined' ? localStorage.getItem(warningToggleKey) : null;
if (storedWarning !== null) warningEnabled = storedWarning === 'true';
let deviceStatus = { status: 'RUN', lastTick: null };
let lastWarningTime = 0; // 마지막 경고 팝업이 뜬 시간 (밀리초)
const WARNING_COOLDOWN_MS = 60 * 1000; // 1분 쿨다운
let pageStartTime = Date.now(); // 페이지 로드 시간 (또는 장비 시작 시간)
const STARTUP_GRACE_PERIOD_MS = 5 * 1000; // 시작 후 5초 그레이스 기간
const displayParam = (name) => {
    // MFC1_N2-1 -> N2-1, MFC7_DCS -> DCS 등으로 변환
    const m = String(name || '').match(/^MFC\d+[_-](.+)$/i);
    return m && m[1] ? m[1] : name;
};
function applyDisplayLabels() {
    document.querySelectorAll('.chart-row').forEach(row => {
        const param = row.dataset.param;
        const labelEl = row.querySelector('.param-label');
        if (labelEl) labelEl.textContent = displayParam(param);
        row.querySelectorAll('.expand-btn').forEach(btn => {
            btn.setAttribute('aria-label', `${displayParam(param)} 차트 확대`);
        });
    });
}

let latestStepTimestamp = 0;
let lastStepUpdate = 0;
let lastStepFallbackFetch = 0;
let warningElements = {};
let reportElements = {};
let currentReportEntry = null;
let reportChart = null;
const mfcParams = new Set(['MFC7_DCS', 'MFC8_NH3', 'MFC1_N2-1', 'MFC2_N2-2', 'MFC3_N2-3', 'MFC4_N2-4']);
let threeViewer = null;
const htmlCache = new Map();
let limits = {};
let interlockLimits = {};

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

function hasColumn(col) {
    return columns.includes(col);
}
const partsCatalog = {
    default: {
        title: '부품 정보',
        image: null,
        usage: '설치 상태, 마모/누설 여부, 교정 기록을 확인해 주세요.',
        feature: '',
        principle: '',
        vendors: []
    },
    byViolation: {
        1: {
            title: 'Baratron Gauge 부품 정보',
            image: { src: '/static/img/Baratron%20Gauge.png', alt: 'Baratron Gauge', caption: 'Baratron Gauge' },
            description: '- Baratron Gauge는 얇은 다이어프램의 정전용량 변화를 이용해 챔버의 절대압을 고정밀로 측정하는 정전용량식 압력계.\n- 가스 조성이나 온도 변화의 영향을 거의 받지 않아 반도체 진공 환경에서 가장 높은 신뢰성을 제공하는 압력 측정 장비.\n- Etch, CVD, ALD 등 미세 압력 제어가 중요한 공정에서 공정 안정성·재현성을 확보하기 위한 핵심 계측 장비.',
            sales: {
                iframeUrl: 'https://www.ebay.com/itm/326797587695',
                imageUrl: '/static/img/link_1.png',
                text: '[이상 로그 연계 판단]\n본 이상 로그의 원인 후보 중\'Baratron Gauge 성능 저하\'가능성 높음\n동일 증상 발생 시 본 부품 교체 후 정상화 이력 존재'
            },
            vendors: [
                { name: '제우스', biz: '2298105323', link: 'https://www.globalzeus.com/kr/index.asp', contact: 'TEL: 031-5187-1774 E MAIL: vacuum_2@globalzeus.com' },
                { name: '다이나믹 세미텍', biz: '5658800577', link: 'https://dynamicsemi.co.kr/?act=main', contact: 'TEL: 054-437-2061' }
            ],
            // 수정 전 목록 (violation_type 1일 때 사용)
            oldVendors: [
                { name: 'MKS', biz: '1268179956', link: 'https://www.mks.com', contact: 'TEL: 031-695-9200' },
                { name: '브이시스', biz: '1428144975', link: 'http://www.vsyskor.com', contact: 'TEL: 031-8067-7750' },
                { name: 'ZEUS', biz: '2298105323', link: 'https://www.globalzeus.com', contact: 'TEL: 031-5187-1774' },
                { name: '다이나믹세미텍', biz: '5658800577', link: 'https://dynamicsemi.co.kr', contact: 'TEL: 054-437-2062' }
            ]
        },
        2: {
            title: 'MFC 부품 정보',
            image: { src: '/static/img/MFC.png', alt: 'MFC', caption: 'MFC' },
            description: '- Alicat MCE/MCV 압력 기반 Mass Flow Controller는 0.01–100%의 넓은 제어 범위와 1ms 미만의 초고속 센서 응답을 제공하는 고정밀 유량 제어 장치\n- 압력·온도·질량·체적 유량의 다변량 측정이 가능하며 ±0.6% of reading의 높은 정확도를 유지해 안정적인 공정 제어를 보장\n- 다중 가스 호환, 내장 디스플레이, 다양한 산업용 통신 프로토콜을 지원해 반도체 및 진공 공정에서 높은 신뢰성과 호환성을 제공',
            sales: {
                iframeUrl: 'https://www.ebay.com/itm/182728947822',
                imageUrl: '/static/img/link_2.png',
                text: '[이상 로그 연계 판단]\n- 본 이상 로그의 원인 후보 중 \'MFC 성능 저하\'가능성 높음\n- 동일 증상 발생 시 본 부품 교체 후 정상화 이력 존재'
            },
            vendors: [
                { name: 'MKP', biz: '2308700261', link: 'https://www.mkpsemi.com/main', contact: 'TEL: +82-31-613-3359' },
                { name: 'Flotron', biz: '2148837332', link: 'http://www.flotron.co.kr/', contact: 'TEL: 02-3470-5800 E MAIL: sales@flotron.co.kr' },
                { name: '삼일산업', biz: '1398129658', link: 'http://www.samilind.co.kr/', contact: 'TEL: 032-819-9671 E MAIL: samilind79@samilind.co.kr' },
                { name: '샘시스템', biz: '2118741242', link: 'http://www.semsys.co.kr/', contact: 'TEL : 1899-8016,8827 E MAIL: sem@semsys.co.kr' },
                { name: '서진 인스텍', biz: '2308700261', link: 'https://www.seojin.biz/sj/sub/main.php', contact: 'TEL: 031-627-9000 / 9010 / 9011' },
                { name: '세바', biz: '5138112850', link: 'http://www.seba.co.kr/', contact: 'TEL: (054) 712-5200 E MAIL: seba@seba.co.kr' },
                { name: '세화 하이테크', biz: '1308600547', link: 'https://www.gasplus.com/', contact: 'TEL: 032-624-3800 E MAIL: sehwa@gasplus.com' },
                { name: '송도산업', biz: '1298654612', link: 'https://song-do.co.kr/', contact: 'TEL: 031-742-3909' }
            ]
        },
        3: {
            title: 'Magnetic Seal 부품 정보',
            image: { src: '/static/img/M_Seal.png', alt: 'Magnetic Seal', caption: 'M.Seal' },
            description: '- M.SEAL은 반도체 장비의 펌프·모터·챔버 등에서 유체와 가스 누설을 방지하는 **기계식 씰(Mechanical Seal)**을 의미하는 핵심 부품.\n- 고온·고압·화학환경에서 회전체와 고정체 사이의 밀폐를 유지하며 공정 안정성을 확보하는 구조.열화 시 진동, 유량, 압력 변화로 이어져 장비 성능 저하와 고장 예측의 주요 지표가 되는 요소.\n- TSV·세정·CMP·ALD 등 다양한 공정 장비에서 필수적으로 사용되는 밀폐 구성.',
            sales: {
                iframeUrl: 'https://www.ebay.com/itm/267325335220',
                imageUrl: '/static/img/link_3.png',
                text: '[이상 로그 연계 판단]\n본 이상 로그의 원인 후보 중 \'M.Seal 성능 저하\'가능성 높음\n동일 증상 발생 시 본 부품 교체 후 정상화 이력 존재'
            },
            vendors: [
                { name: '서울테크', biz: '8248100138', link: 'https://www.seoul-tech.kr/eng/', contact: 'TEL : +82-32-661-1888 E MAIL: soulteclky@naver.com' },
                { name: '한성테크', biz: '6758601622', link: 'https://www.sealhs.co.kr/', contact: 'TEL : 051-319-2211 EMAIL : info@sealhs.com' }
            ],
            // 수정 전 목록 (violation_type 3일 때 사용)
            oldVendors: [
                { name: '마그넥스', biz: '1248610394', link: 'https://www.magnex.co.kr', contact: 'TEL: 043-276-8598' },
                { name: '디노솔루션', biz: '1358632836', link: 'https://dinosolution.co.kr', contact: 'TEL: 031-206-6406' },
                { name: 'LOTCES', biz: '1358628690', link: 'https://lotces.com', contact: 'TEL: 041-548-6540' },
                { name: 'KSM', biz: '1378607202', link: 'https://www.ksm.co.kr', contact: 'TEL: 031-983-7700' }
            ]
        },
        4: {
            title: 'CKD 밸브 부품 정보',
            image: { src: '/static/img/CKD.png', alt: 'CKD Valve', caption: 'CKD' },
            description: '- CKD Valve는 일본 CKD(CKD Corporation)에서 제조하는 산업용 공압·유압·유체 제어 밸브로, 반도체·디스플레이·자동화 설비에서 널리 사용되는 정밀 제어 부품.\n- 고청정·내식성·내열 특성을 갖춘 모델이 많아 DIW, 케미컬, 가스 라인 등 공정 배관의 안정적 개폐와 유량 제어에 사용되는 요소.\n- 응답 속도, 내구성, 누설률 관리가 우수하여 자동화 장비의 반복 동작에서도 높은 신뢰성을 보장하는 제품.반도체용 퍼지 밸브·솔레노이드 밸브·피팅류 등 다양한 시리즈 라인업을 가진 밸브 제품군.',
            sales: {
                iframeUrl: 'https://www.ebay.com/itm/389248699124',
                imageUrl: '/static/img/link_4.png',
                text: '[이상 로그 연계 판단]\n본 이상 로그의 원인 후보 중 \'CKD 성능 저하\'가능성 높음\n동일 증상 발생 시 본 부품 교체 후 정상화 이력 존재'
            },
            vendors: [
                { name: 'CKD Korea', biz: '1208609538', link: 'https://www.ckdkorea.co.kr/', contact: 'TEL: 02-783-5201 E MAIL: ckdkorea@ckd-k.co.kr' }
            ],
            // 수정 전 목록 (violation_type 4일 때 사용)
            oldVendors: [
                { name: '삼인CKD', biz: '1388106721', link: 'http://www.samin4u.com', contact: 'TEL: 031-433-9922' },
                { name: 'NAT', biz: '1288119363', link: 'http://www.nat21.co.kr', contact: 'TEL: 02-2676-4483' },
                { name: '한국도키멕유공압', biz: '1188103618', link: 'http://www.tokimec.co.kr', contact: 'TEL: 070-7123-4603' },
                { name: 'Inatech&CORP', biz: '1068142171', link: 'https://www.inacorp.co.kr', contact: 'TEL: 02-2026-0660' }
            ]
        }
    }
};

const processSteps = [
    {
        title: '1. SI-FL1 Step',
        subtitle: 'Silicon Flow 1: Source Injection & Adsorption',
        image: { src: '/static/img/step_1.png', alt: 'Silicon Source Feed', caption: 'Silicon Source Feed' },
        sections: [
            {
                label: '공정 동작',
                text: '다이클로로실레인(SiH2Cl2, DCS)이 저장된 Si Source Tank와 반응 챔버 사이의 밸브가 순간적으로 열립니다. 탱크 내의 높은 압력(Charge된 상태)을 이용해 DCS 가스가 챔버 내부로 빠르게 분사됩니다.'
            },
            {
                label: '화학적 메커니즘 (Chemisorption)',
                text: '웨이퍼 표면(이전 사이클로 인해 NHx 또는 OH 기로 마감된 상태)에 DCS 분자가 도달합니다. DCS는 표면의 활성 자리(Active Site)와 반응하여 화학 흡착(Chemisorption)을 일으킵니다. 자기 제한적 반응(Self-Limiting): 표면의 모든 활성 자리가 DCS 분자로 덮이면(1 Monolayer 포화), 더 이상 물리적으로 흡착된 가스들은 표면과 결합하지 못하고 겉돌게 됩니다.'
            },
            {
                label: '핵심 포인트',
                text: '가스 유량이 너무 적으면 표면 포화가 안 되어 박막 성장률(GPC)이 떨어지고, 너무 많으면 파티클의 원인이 되므로 Source Tank의 압력 제어와 투입 시간(Feeding Time) 최적화가 필수적입니다.'
            }
        ]
    },
    {
        title: '2. SI-EVA1 Step',
        subtitle: 'Silicon Evacuation 1: Purge & Desorption',
        image: { src: '/static/img/step_2.png', alt: 'Silicon Source Purge', caption: 'Silicon Source Purge' },
        sections: [
            {
                label: '공정 동작',
                text: 'DCS 공급을 차단하고, 불활성 가스인 질소(N2)를 챔버 내로 강하게 불어넣습니다(Purge). 동시에 진공 펌프를 통해 챔버 내부 기체를 배기(Evacuation)합니다.'
            },
            {
                label: '물리/화학적 메커니즘',
                text: '표면에 화학적으로 강하게 결합하지 못한 물리 흡착(Physisorption) 분자들과 잉여 DCS 가스를 챔버 밖으로 밀어냅니다. 이 단계가 불완전하면 잔류 DCS가 다음 단계의 NH3와 기상(Gas-phase)에서 반응하여 파티클(Powder)을 형성하거나, ALD가 아닌 CVD 거동을 보여 박막 두께 균일도(Uniformity)를 해치게 됩니다.'
            },
            {
                label: '핵심 포인트',
                text: '완벽한 퍼지를 통해 오직 표면에 단단히 결합한 1층의 Si 전구체만 남기는 것이 이 스텝의 핵심입니다.'
            }
        ]
    },
    {
        title: '3. N-FL1 Step',
        subtitle: 'Nitrogen Flow 1: Plasma Ignition & Radical Generation',
        image: { src: '/static/img/step_3.png', alt: 'NH3 Source Feed + Plasma On', caption: 'NH3 Source Feed + Plasma On' },
        sections: [
            {
                label: '공정 동작',
                text: 'NH3 가스가 챔버 상단의 Plasma Area로 주입됩니다. 동시에 RF Power가 인가되어 NH3를 분해합니다.'
            },
            {
                label: '화학적 메커니즘 (Radical Reaction)',
                text: 'NH3 가스는 열에너지로는 저온에서 잘 분해되지 않기 때문에, RF 플라즈마 에너지를 이용해 고반응성 질소 라디칼(N*, NH*, NH* 등)로 분해됩니다. 생성된 라디칼들은 Plasma Area에서 Wafer가 있는 반응 영역(Reaction Zone)으로 이동합니다. 질소 라디칼은 웨이퍼 표면에 흡착된 DCS의 리간드(-Cl, -H)를 공격하여 떨어뜨리고, 그 자리에 질소가 결합하며 Si-N 결합을 형성합니다. (부산물로 HCl, H2 가스 생성)'
            },
            {
                label: '핵심 포인트',
                text: '웨이퍼에 직접 플라즈마를 때리는 것이 아니라, 상부에서 생성된 라디칼을 이용(Remote Plasma 방식)함으로써 기판 손상(Plasma Damage)을 최소화합니다.'
            }
        ]
    },
    {
        title: '4. N-FL2 Step',
        subtitle: 'Nitrogen Flow 2: Reaction Saturation & Precursor Refill',
        image: { src: '/static/img/step_4.png', alt: 'NH3 Source Feed + Plasma On + Si Tank Charge', caption: 'NH3 Source Feed + Plasma On + Si Tank Charge' },
        sections: [
            {
                label: '공정 동작',
                text: '<strong>반응 챔버 :</strong><br>NH3 공급과 RF Power는 계속 유지됩니다. N 라디칼이 웨이퍼 표면의 미처 반응하지 못한 구석구석까지 침투하여 Si-N 반응을 포화(Saturation) 상태로 만듭니다.<br><br><strong>가스 라인 (Back-end) :</strong><br>이와 동시에, 다음 사이클을 준비하기 위해 SiH2Cl2 공급 라인(MFC 후단)에서 비어 있는 Si Source Tank로 DCS를 다시 채워넣는(Charge/Refill) 과정이 일어납니다.'
            },
            {
                label: '목적',
                text: '공정 시간을 단축(Throughput 향상)하기 위해 반응 시간(Reaction Time)을 확보함과 동시에 전구체 충전(Bottling)을 병렬로 수행하는 효율적인 단계입니다.'
            }
        ]
    },
    {
        title: '5. N-EVA1 Step',
        subtitle: 'Nitrogen Evacuation 2: Byproduct Removal & Film Formation',
        image: { src: '/static/img/step_5.png', alt: 'Si3N4 생성 + Byproduct 휘발', caption: 'Si3N4 생성 + Byproduct 휘발' },
        sections: [
            {
                label: '공정 동작',
                text: 'RF Power를 끄고 NH3 공급을 중단합니다. 다시 N2 가스를 이용하여 챔버를 퍼지하고 배기합니다.'
            },
            {
                label: '물리/화학적 메커니즘',
                text: '반응 후 생성된 부산물 (HCl 가스 등)과 반응에 참여하지 않고 남은 잉여 N라디칼, NH3 가스를 제거합니다. 이 과정이 끝나면 웨이퍼 표면은 다시 NHx 등의 활성기로 덮인 상태가 되어, 다음 사이클의 DCS와 반응할 준비가 완료됩니다.'
            },
            {
                label: '최종 결과',
                text: '이 5단계를 거치면 웨이퍼 위에는 불순물이 적고 두께가 매우 균일한 단원자층 두께의 Si3N4 박막이 형성됩니다. 이를 원하는 두께가 될 때까지 수십~수백 회 반복합니다.'
            }
        ]
    }
];

function updateCurrentStepDisplay(stepId, stepName) {
    const idEl = document.getElementById('current-step-id');
    const nameEl = document.getElementById('current-step-name');
    if (idEl) {
        idEl.textContent = stepId !== null && stepId !== undefined ? stepId : '-';
    }
    if (nameEl) nameEl.textContent = 'DEPO';
    lastStepUpdate = Date.now();
}

function considerActualStepInfo(actual) {
    for (let i = actual.length - 1; i >= 0; i--) {
        const entry = actual[i];
        if (!entry) continue;
        const hasInfo = (entry.step_id !== null && entry.step_id !== undefined) || (entry.step_name && entry.step_name !== '');
        if (!hasInfo) continue;
        const timeValue = new Date(entry.x).getTime();
        if (Number.isNaN(timeValue)) continue;
        if (timeValue > latestStepTimestamp) {
            latestStepTimestamp = timeValue;
            updateCurrentStepDisplay(entry.step_id ?? null, entry.step_name ?? null);
        }
        break;
    }
}

async function fetchCurrentStepFallback(force = false) {
    const now = Date.now();
    if (!force && now - lastStepUpdate < 3000) {
        return;
    }
    if (!force && now - lastStepFallbackFetch < 3000) {
        return;
    }
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

async function loadSettings() {
    try {
        const res = await fetch('/api/settings');
        if (!res.ok) return;
        const s = await res.json();
        settingsCache = s || {};
        if (typeof s.warning_enabled === 'boolean') {
            warningEnabled = s.warning_enabled;
        }
    } catch (e) {
        console.error('failed to load settings', e);
    }
}

async function saveWarningSetting() {
    const body = { ...settingsCache, warning_enabled: warningEnabled };
    try {
        await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        settingsCache = body;
    } catch (e) {
        console.error('failed to save warning toggle', e);
    }
}

const visibilityKey = 'chartVisibilityMode';
// 새로고침 시 항상 '실제값만'으로 시작하도록 설정
let visibilityMode = 'actual'; // 기본값을 '실제값만'으로 변경
// localStorage에는 저장하지 않아서 매번 새로고침 시 기본값으로 시작
const visibilityLabels = {
    both: '실제값 + 예측값',
    actual: '실제값만',
    predicted: '예측값만'
};

function setDatasetVisibility(chart, mode) {
    const pred = chart.getDatasetMeta(0);
    const act = chart.getDatasetMeta(1);
    if (mode === 'actual') {
        pred.hidden = true;
        act.hidden = false;
    } else if (mode === 'predicted') {
        pred.hidden = false;
        act.hidden = true;
    } else {
        pred.hidden = false;
        act.hidden = false;
    }
}

function applyVisibilityAll() {
    Object.values(mainCharts).forEach(c => { setDatasetVisibility(c, visibilityMode); c.update(); });
}

const highlightPlugin = {
    id: 'highlightRegion',
    beforeDatasetsDraw(chart, args, opts) {
        const { ctx, chartArea: { top, bottom }, scales: { x } } = chart;
        ctx.save();
        ctx.fillStyle = 'rgba(173,255,47,0.4)';
        (opts.regions || []).forEach(r => {
            const xStart = x.getPixelForValue(r.start);
            const xEnd = x.getPixelForValue(r.end);
            ctx.fillRect(xStart, top, xEnd - xStart, bottom - top);
        });
        ctx.restore();
    }
};
Chart.register(highlightPlugin);

function formatTime(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return '-';
    return d.toLocaleTimeString('ko-KR', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function toMillis(ts) {
    if (ts === null || ts === undefined) return null;
    const num = Number(ts);
    if (Number.isFinite(num)) {
        // 10자리 초 단위 입력을 ms로 변환
        if (num > 0 && num < 1e11) return num * 1000;
        return num;
    }
    const d = new Date(ts);
    const v = d.getTime();
    return Number.isNaN(v) ? null : v;
}

function parseTimestamp(ts) {
    if (ts instanceof Date) return ts;
    if (typeof ts === 'number') return new Date(ts);
    if (typeof ts === 'string') {
        // 공백 구분 포맷을 ISO로 치환
        return new Date(ts.replace(' ', 'T'));
    }
    return new Date(ts);
}

function safeId(name) {
    return name.replace(/[ .-]/g, '_');
}

function setDeviceState(status, lastTick) {
    const toggle = document.getElementById('device-toggle');
    const runEl = document.getElementById('device-state-run');
    const downEl = document.getElementById('device-state-down');
    deviceStatus = { status, lastTick };
    if (!toggle || !runEl || !downEl) return;
    const isRun = status === 'RUN';
    toggle.classList.toggle('down-active', !isRun);
    runEl.classList.toggle('active', isRun);
    downEl.classList.toggle('active', !isRun);
}

async function pollGeneratorStatus() {
    try {
        const res = await fetch('/api/generator_status');
        if (!res.ok) throw new Error('bad response');
        const json = await res.json();
        const status = json?.status === 'RUN' ? 'RUN' : 'DOWN';
        setDeviceState(status, json?.last_tick ?? null);
    } catch (e) {
        setDeviceState('DOWN', null);
    }
}

function createCharts() {
    const xAxis = {
        type: 'time',
        time: {
            unit: 'second',
            tooltipFormat: 'HH:mm:ss',
            displayFormats: {
                second: 'HH:mm:ss',
                minute: 'HH:mm:ss'
            }
        },
        ticks: {
            source: 'data',
            autoSkip: true,
            maxTicksLimit: 5,
            maxRotation: 0,
            autoSkipPadding: 12,
            callback: function (value, index, ticks) {
                // 첫 번째 틱(왼쪽)은 표시하지 않음
                if (index === 0) {
                    return '';
                }
                const d = new Date(value);
                if (Number.isNaN(d.getTime())) {
                    return '';
                }
                // 간단한 시간 형식 (HH:mm:ss)
                const hours = String(d.getHours()).padStart(2, '0');
                const minutes = String(d.getMinutes()).padStart(2, '0');
                const seconds = String(d.getSeconds()).padStart(2, '0');
                return `${hours}:${minutes}:${seconds}`;
            }
        },
        adapters: { date: {} }
    };
    columns.forEach(col => {
        const id = safeId(col);
        const ctx = document.getElementById(`main-${id}`).getContext('2d');
        const rowEl = document.querySelector(`.chart-row[data-param="${col}"]`);
        if (rowEl) {
            const boxes = Array.from(rowEl.querySelectorAll('.chart-box'));
            chartBoxesMap[col] = boxes;
            boxes.forEach(box => {
                box.addEventListener('click', () => handleChartClick(col));
            });
        }
        mainCharts[col] = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [
                    { label: '예측값', borderColor: 'red', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] },
                    { label: '실제값', borderColor: 'blue', tension: 0.25, borderWidth: 3, pointRadius: 0, data: [] },
                    { label: '상한선', borderColor: 'green', borderDash: [5, 5], borderWidth: 2, pointRadius: 0, data: [], hidden: true },
                    { label: '하한선', borderColor: 'orange', borderDash: [5, 5], borderWidth: 2, pointRadius: 0, data: [], hidden: true },
                    { label: 'Interlock 상한선', borderColor: 'red', borderDash: [3, 3], borderWidth: 2, pointRadius: 0, data: [], hidden: true },
                    { label: 'Interlock 하한선', borderColor: 'red', borderDash: [3, 3], borderWidth: 2, pointRadius: 0, data: [], hidden: true }
                ]
            },
            options: {
                animation: false,
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    highlightRegion: { regions: [] }
                },
                scales: { x: xAxis, y: {} }
            }
        });
        setDatasetVisibility(mainCharts[col], visibilityMode);
    });
    applyVisibilityAll();
    applyHighlightState();
}

function getEntryKey(param, entry) {
    return `${param}-${entry.start}-${entry.end}`;
}

function formatTimelineTime(ts) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return '-';
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}년 ${pad(d.getMonth() + 1)}월 ${pad(d.getDate())}일 ${pad(d.getHours())}시 ${pad(d.getMinutes())}분 ${pad(d.getSeconds())}초`;
}

function buildLogText(param, entry) {
    const diffRaw = entry.diff != null ? Math.abs(entry.diff) : 0;
    const diff = (diffRaw / 10).toFixed(1);
    let direction = 0;
    if (entry.actual_value != null && entry.predicted_value != null) {
        direction = entry.actual_value - entry.predicted_value;
    }
    let descriptor;
    if (direction > 0.0001) {
        descriptor = `유량 +${diff}% 상승 감지`;
    } else if (direction < -0.0001) {
        descriptor = `유량 -${diff}% 하락 감지`;
    } else {
        descriptor = `유량 편차 ${diff}% 감지`;
    }
    return `${displayParam(param)} ${descriptor}`;
}

function tryParseMessage(msg) {
    if (!msg) return {};
    try {
        return JSON.parse(msg);
    } catch (e) {
        return {};
    }
}

function normalizeLogEntry(item) {
    const parsed = tryParseMessage(item?.message);
    const param = parsed.parameter || item.parameter;
    if (!param) return null;
    const startStr = parsed.start || item.start_time || item.timestamp;
    const endStr = parsed.end || item.end_time || item.timestamp;
    const startMs = toMillis(startStr);
    const endMs = toMillis(endStr);
    return {
        param,
        entry: {
            start: startMs,
            end: endMs,
            diff: parsed.diff_percent ?? parsed.diff ?? item.avg_diff_percent ?? null,
            duration_seconds: parsed.duration_seconds ?? item.duration_seconds ?? null,
            step_id: parsed.step_id || [],
            step_name: parsed.step_name || [],
            actual_value: parsed.actual_value ?? null,
            predicted_value: parsed.predicted_value ?? null,
            peak_time: toMillis(parsed.peak_time),
            violation_type: parsed.violation_type ?? item.violation_type ?? null,
        },
    };
}

function setServerLogs(list) {
    logs = {};
    const prevMax = lastServerLogEnd;
    lastServerLogEnd = 0;
    const newEntries = [];
    list.forEach(item => {
        const norm = normalizeLogEntry(item);
        if (!norm) return;
        if (!logs[norm.param]) logs[norm.param] = [];
        logs[norm.param].push(norm.entry);
        if (norm.entry.end) {
            lastServerLogEnd = Math.max(lastServerLogEnd, norm.entry.end);
            if (norm.entry.end > prevMax) newEntries.push({ param: norm.param, entry: norm.entry });
        }
    });
    Object.values(logs).forEach(arr => arr.sort((a, b) => b.start - a.start));
    updateLog();
    applyHighlightState();

    if (warningEnabled && !warningModalOpen && newEntries.length) {
        const first = newEntries[0];
        console.log(`[경고 팝업 호출] 파라미터: ${first.param}, 새 항목 수: ${newEntries.length}`);
        openWarningModal(first.param, buildLogText(first.param, first.entry));
    } else if (newEntries.length) {
        console.log(`[경고 팝업 스킵] warningEnabled: ${warningEnabled}, warningModalOpen: ${warningModalOpen}, newEntries.length: ${newEntries.length}`);
    }
    if (lastServerLogEnd > lastSeenLogEnd) {
        lastSeenLogEnd = lastServerLogEnd;
    }
}

function getHighlightRegions(param, startMs, endMs) {
    const entries = logs[param] || [];
    if (startMs == null || endMs == null) return [];
    return entries.map(e => ({
        start: Math.max(e.start ?? -Infinity, startMs),
        end: Math.min(e.end ?? Infinity, endMs),
    })).filter(r => Number.isFinite(r.start) && Number.isFinite(r.end) && r.end > r.start);
}

function updateTimeRangeLabel(startMs, endMs) {
    const el = document.getElementById('chart-time-range');
    if (!el) return;
    el.textContent = `조회 구간: ${formatTimelineTime(startMs)} ~ ${formatTimelineTime(endMs)}`;
}

function applyHighlightState() {
    Object.values(chartBoxesMap).forEach(arr => {
        if (!arr) return;
        arr.forEach(box => box.classList.remove('highlight'));
    });
    logEntryElements.forEach(el => el.classList.remove('highlight'));
    if (!currentHighlight) return;
    const { param, key } = currentHighlight;
    if (chartBoxesMap[param]) {
        chartBoxesMap[param].forEach(box => box.classList.add('highlight'));
    }
    logEntryElements.forEach((el, entryKey) => {
        const matchesKey = key && entryKey === key;
        const matchesParam = key === null && el.dataset.param === param;
        if (matchesKey || matchesParam) {
            el.classList.add('highlight');
        }
    });
}

function setHighlight(param, key, options = {}) {
    if (currentHighlight && currentHighlight.param === param && currentHighlight.key === key) {
        currentHighlight = null;
    } else {
        currentHighlight = { param, key };
    }
    applyHighlightState();
    const active = currentHighlight && currentHighlight.param;
    const shouldScrollLog = options.scrollLog || options.scroll;
    if (active && options.scrollChart) {
        const boxes = chartBoxesMap[currentHighlight.param];
        if (boxes && boxes.length) {
            boxes[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
    if (active && shouldScrollLog) {
        let target = null;
        if (currentHighlight.key && logEntryElements.has(currentHighlight.key)) {
            target = logEntryElements.get(currentHighlight.key);
        }
        if (!target) {
            logEntryElements.forEach(el => {
                if (el.dataset.param !== currentHighlight.param) return;
                if (!target || el.offsetTop < target.offsetTop) target = el;
            });
        }
        if (target) {
            target.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

function handleLogClick(param, key) {
    setHighlight(param, key, { scrollChart: true });
}

function handleChartClick(param) {
    const entries = logs[param];
    if (!entries || !entries.length) {
        setHighlight(param, null);
        return;
    }
    // 전체 로그를 강조하기 위해 key는 null로 설정
    const isActive = currentHighlight && currentHighlight.param === param && currentHighlight.key === null;
    setHighlight(param, null, { scrollLog: !isActive });
}

function updateLog() {
    const logDiv = document.getElementById('log-content');
    if (!logDiv) return;
    logEntryElements.clear();
    logDiv.innerHTML = '';
    const allEntries = [];
    Object.entries(logs).forEach(([param, arr]) => {
        arr.forEach(entry => {
            allEntries.push({ param, ...entry });
        });
    });
    allEntries.sort((a, b) => b.end - a.end);
    allEntries.forEach(entry => {
        const key = getEntryKey(entry.param, entry);
        const wrapper = document.createElement('div');
        wrapper.className = 'timeline-entry';
        wrapper.dataset.param = entry.param;
        wrapper.dataset.key = key;
        const timeEl = document.createElement('div');
        timeEl.className = 'timeline-time';
        timeEl.textContent = formatTimelineTime(entry.end);
        const textEl = document.createElement('div');
        textEl.className = 'timeline-text';
        textEl.textContent = buildLogText(entry.param, entry);
        const iconEl = document.createElement('div');
        iconEl.className = 'timeline-icon';
        iconEl.textContent = '⚠';
        const reportBtn = document.createElement('button');
        reportBtn.className = 'report-btn';
        reportBtn.textContent = 'Report';
        reportBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            openReportModal(entry);
        });
        const bodyEl = document.createElement('div');
        bodyEl.className = 'timeline-body';
        bodyEl.appendChild(timeEl);
        bodyEl.appendChild(textEl);
        wrapper.appendChild(iconEl);
        wrapper.appendChild(bodyEl);
        wrapper.appendChild(reportBtn);
        wrapper.addEventListener('click', () => handleLogClick(entry.param, key));
        logDiv.appendChild(wrapper);
        logEntryElements.set(key, wrapper);
    });
    if (currentHighlight && currentHighlight.key && !logEntryElements.has(currentHighlight.key)) {
        currentHighlight = null;
    }
    applyHighlightState();
}

function updateLogPanelHeight() {
    const chartsEl = document.getElementById('charts-container');
    const logPanel = document.getElementById('log-panel');
    if (chartsEl && logPanel) {
        logPanel.style.maxHeight = chartsEl.offsetHeight + 'px';
    }
}

function trimLogEntries(limit = 20) {
    const all = [];
    Object.entries(logs).forEach(([param, arr]) => {
        arr.forEach(l => all.push({ param, ...l }));
    });
    all.sort((a, b) => b.start - a.start);
    const trimmed = all.slice(0, limit);
    logs = {};
    loggedIds.clear();
    trimmed.forEach(l => {
        if (!logs[l.param]) logs[l.param] = [];
        logs[l.param].push({
            start: l.start,
            end: l.end,
            diff: l.diff,
            step_id: l.step_id,
            step_name: l.step_name,
            actual_value: l.actual_value,
            predicted_value: l.predicted_value,
            peak_time: l.peak_time
        });
        loggedIds.add(`${l.param}-${l.start}-${l.end}`);
    });
    Object.values(logs).forEach(arr => arr.sort((a, b) => b.start - a.start));
}


function updateCharts(col, data) {
    const actual = data.actual.map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    const predicted = data.predicted.map(d => ({ ...d, x: parseTimestamp(d.x), y: d.y }));
    considerActualStepInfo(actual);

    const chart = mainCharts[col];
    if (!chart) return;

    // fetchData에서 이미 시간 범위로 필터링된 데이터를 받았으므로
    // 추가 필터링 없이 그대로 사용
    chart.data.datasets[0].data = predicted;
    chart.data.datasets[1].data = actual;

    // 경고팝업토글 ON시 상한선/하한선 표시
    if (warningEnabled && limits && limits[col]) {
        const upperLimit = [];
        const lowerLimit = [];
        const allData = actual.concat(predicted);

        // allData가 비어있으면 시간 범위로 생성
        let timePoints = [];
        if (allData.length > 0) {
            timePoints = allData.map(d => d.x);
        } else {
            // 데이터가 없어도 시간 범위는 표시
            const now = Date.now();
            const timeRangeMs = selectedTimeRange * 60 * 1000;
            const startTime = now - timeRangeMs;
            timePoints = [new Date(startTime), new Date(now)];
        }

        // 가장 최근 데이터의 step_id를 사용하거나, 없으면 'all' 사용
        let currentStep = 'all';
        if (actual.length > 0 && actual[actual.length - 1].step_id !== undefined) {
            currentStep = actual[actual.length - 1].step_id?.toString() || 'all';
        } else if (predicted.length > 0 && predicted[predicted.length - 1].step_id !== undefined) {
            currentStep = predicted[predicted.length - 1].step_id?.toString() || 'all';
        }

        // limits[col]이 존재하는지 확인
        const colLimits = limits[col];
        if (!colLimits) {
            // limits.yaml에 없는 파라미터는 경고 없이 넘어감 (정상적인 경우)
            chart.data.datasets[2].data = [];
            chart.data.datasets[3].data = [];
            chart.data.datasets[2].hidden = true;
            chart.data.datasets[3].hidden = true;
            return;
        }

        // currentStep의 limit을 먼저 확인하고, 없거나 빈 객체이거나 max/min이 없으면 'all' 사용
        let limit = colLimits[currentStep];
        // currentStep의 limit이 없거나, 빈 객체이거나, max/min이 모두 없으면 'all' 사용
        if (!limit || Object.keys(limit).length === 0 || (limit.max === undefined && limit.min === undefined)) {
            limit = colLimits['all'];
        }

        if (limit && (limit.max !== undefined || limit.min !== undefined)) {
            timePoints.forEach(x => {
                if (limit.max !== undefined && limit.max !== null) {
                    upperLimit.push({ x: x, y: limit.max });
                }
                if (limit.min !== undefined && limit.min !== null) {
                    lowerLimit.push({ x: x, y: limit.min });
                }
            });

            // 성공적으로 상한선/하한선 데이터 생성됨
        } else {
            // 'all'도 없거나 max/min이 없는 경우
            console.warn(`[상한선/하한선] ${col} - limit이 없거나 max/min이 없습니다. limit:`, limit, `currentStep:`, currentStep, `colLimits keys:`, Object.keys(colLimits));
        }

        chart.data.datasets[2].data = upperLimit;
        chart.data.datasets[3].data = lowerLimit;
        chart.data.datasets[2].hidden = !warningEnabled || upperLimit.length === 0;
        chart.data.datasets[3].hidden = !warningEnabled || lowerLimit.length === 0;
    } else {
        if (!warningEnabled) {
            console.log(`[상한선/하한선] ${col} - 경고팝업이 OFF입니다.`);
        } else if (!limits) {
            console.warn(`[상한선/하한선] ${col} - limits가 로드되지 않았습니다.`);
        } else if (!limits[col]) {
            // limits.yaml에 없는 파라미터는 경고 없이 넘어감
        }
        chart.data.datasets[2].data = [];
        chart.data.datasets[3].data = [];
        chart.data.datasets[2].hidden = true;
        chart.data.datasets[3].hidden = true;
    }

    // Interlock 상한선/하한선 표시 (빨간색)
    if (warningEnabled && interlockLimits && interlockLimits[col]) {
        const interlockUpperLimit = [];
        const interlockLowerLimit = [];
        const allData = actual.concat(predicted);

        // allData가 비어있으면 시간 범위로 생성
        let timePoints = [];
        if (allData.length > 0) {
            timePoints = allData.map(d => d.x);
        } else {
            // 데이터가 없어도 시간 범위는 표시
            const now = Date.now();
            const timeRangeMs = selectedTimeRange * 60 * 1000;
            const startTime = now - timeRangeMs;
            timePoints = [new Date(startTime), new Date(now)];
        }

        // Interlock은 'all'만 사용
        const interlockLimit = interlockLimits[col]['all'];

        if (interlockLimit && (interlockLimit.max !== undefined || interlockLimit.min !== undefined)) {
            timePoints.forEach(x => {
                if (interlockLimit.max !== undefined && interlockLimit.max !== null) {
                    interlockUpperLimit.push({ x: x, y: interlockLimit.max });
                }
                if (interlockLimit.min !== undefined && interlockLimit.min !== null) {
                    interlockLowerLimit.push({ x: x, y: interlockLimit.min });
                }
            });
        }

        chart.data.datasets[4].data = interlockUpperLimit;
        chart.data.datasets[5].data = interlockLowerLimit;
        chart.data.datasets[4].hidden = !warningEnabled || interlockUpperLimit.length === 0;
        chart.data.datasets[5].hidden = !warningEnabled || interlockLowerLimit.length === 0;
    } else {
        chart.data.datasets[4].data = [];
        chart.data.datasets[5].data = [];
        chart.data.datasets[4].hidden = true;
        chart.data.datasets[5].hidden = true;
    }

    // X축 범위 설정: 현재 시간까지 표시되도록
    const now = Date.now();
    const timeRangeMs = selectedTimeRange * 60 * 1000; // 분을 밀리초로 변환
    const allTimestamps = actual.concat(predicted).map(d => new Date(d.x).getTime()).filter(ts => !isNaN(ts));
    let xMin = null;
    let xMax = null;

    if (allTimestamps.length) {
        xMin = Math.min(...allTimestamps);
        const dataMax = Math.max(...allTimestamps);
        // xMax는 데이터의 최대값과 현재 시간 중 더 큰 값으로 설정
        // 이렇게 하면 데이터가 현재 시간보다 이전이어도 차트가 현재 시간까지 표시됨
        xMax = Math.max(dataMax, now);
        chart.options.scales.x.min = xMin;
        chart.options.scales.x.max = xMax;
    } else {
        // 데이터가 없어도 현재 시간 범위는 표시
        xMin = now - timeRangeMs;
        xMax = now;
        chart.options.scales.x.min = xMin;
        chart.options.scales.x.max = xMax;
    }

    const regions = xMin !== null && xMax !== null ? getHighlightRegions(col, xMin, xMax) : [];
    chart.options.plugins.highlightRegion.regions = regions;

    const allVals = actual.concat(predicted).map(d => d.y);
    if (allVals.length) {
        const max = Math.max(...allVals);
        const min = Math.min(...allVals);
        let pad = 3;
        if (col.startsWith('Temp_Act')) pad = 100;
        else if (col.includes('VG11')) pad = 1;
        else if (col.includes('POS')) pad = 10;
        chart.options.scales.y.max = max + pad;
        chart.options.scales.y.min = min - pad;
    }

    setDatasetVisibility(chart, visibilityMode);

    // 상한선/하한선이 추가되었는지 확인
    if (chart.data.datasets.length >= 4) {
        const upperHidden = chart.data.datasets[2].hidden;
        const lowerHidden = chart.data.datasets[3].hidden;
        const upperData = chart.data.datasets[2].data.length;
        const lowerData = chart.data.datasets[3].data.length;
        if (warningEnabled && (upperData > 0 || lowerData > 0)) {
            console.log(`[상한선/하한선] ${col} - 차트 업데이트 전: 상한선 숨김=${upperHidden}, 데이터=${upperData}, 하한선 숨김=${lowerHidden}, 데이터=${lowerData}`);
        }
    }

    chart.update();

    refreshModalIfNeeded(col);
}

function cloneDatasets(chart) {
    return chart.data.datasets.map(ds => ({
        ...ds,
        data: ds.data.map(p => ({ ...p }))
    }));
}

function refreshModalIfNeeded(col) {
    if (!modalInfo || modalFrozen || modalInfo.param !== col) return;
    // HOLD가 켜져있으면 모달 차트도 업데이트하지 않음
    if (chartHoldEnabled) return;
    renderModalChart(modalInfo.param, modalInfo.kind, false);
}

function renderModalChart(param, kind, showModal) {
    const modal = document.getElementById('chart-modal');
    const title = document.getElementById('modal-title');
    const container = document.getElementById('modal-echart');
    const fallbackCanvas = document.getElementById('modal-canvas');
    if (!modal || !title || !container || !fallbackCanvas) return;
    const source = mainCharts[param];
    if (!source) return;
    let datasets = cloneDatasets(source);
    // visibilityMode에 따라 데이터셋 필터링
    if (visibilityMode === 'actual') {
        datasets = datasets.filter(ds => ds.label === '실제값');
    } else if (visibilityMode === 'predicted') {
        datasets = datasets.filter(ds => ds.label === '예측값');
    }
    // 'both'인 경우 모든 데이터셋 사용
    const regions = source.options?.plugins?.highlightRegion?.regions || [];
    const canUseEcharts = typeof echarts !== 'undefined';

    container.style.display = canUseEcharts ? 'block' : 'none';
    fallbackCanvas.style.display = canUseEcharts ? 'none' : 'block';
    if (showModal) modal.style.display = 'flex';

    if (canUseEcharts) {
        if (!modalChart) {
            modalChart = echarts.init(container);
        }
        const series = datasets.map((ds, idx) => {
            const data = (ds.data || []).map(p => [+new Date(p.x), p.y]);
            const serie = {
                name: ds.label || `series ${idx + 1}`,
                type: 'line',
                showSymbol: false,
                smooth: true,
                data,
                lineStyle: { width: ds.borderWidth || 2, color: ds.borderColor || undefined }
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
            legend: { show: false },
            toolbox: {
                feature: {
                    saveAsImage: {},
                    dataZoom: { yAxisIndex: 'none' },
                    restore: {}
                }
            },
            grid: { left: 50, right: 20, top: 40, bottom: 50 },
            xAxis: {
                type: 'time',
                splitNumber: 5
            },
            yAxis: { type: 'value', scale: true },
            dataZoom: [
                { type: 'inside', xAxisIndex: 0 }
            ],
            series
        };
        modalChart.setOption(option, { notMerge: false, replaceMerge: ['series'], silent: true });
        modalChart.off('dataZoom');
        modalChart.off('restore');
        modalChart.on('dataZoom', () => { modalFrozen = true; });
        modalChart.on('restore', () => { modalFrozen = false; renderModalChart(param, kind, false); });
        setTimeout(() => modalChart?.resize(), 0);
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
                    legend: { display: false },
                    highlightRegion: { regions }
                },
                scales: {
                    x: {
                        type: 'time',
                        ticks: {
                            maxTicksLimit: 5
                        }
                    },
                    y: { type: 'linear' }
                }
            }
        });
        // 확대 차트도 visibilityMode에 따라 연동
        if (modalChartFallback) {
            setDatasetVisibility(modalChartFallback, visibilityMode);
            modalChartFallback.update();
        }
    }
    modalInfo = { param, kind };
    title.textContent = `${displayParam(param)} (최근 ${selectedTimeRange}분)`;
}

function openChartModal(param, kind) {
    modalFrozen = false;
    const modal = document.getElementById('chart-modal');
    const title = document.getElementById('modal-title');
    const container = document.getElementById('modal-echart');
    const fallbackCanvas = document.getElementById('modal-canvas');
    const modalHoldToggle = document.getElementById('modal-chart-hold-toggle');
    if (!modal || !title || !container || !fallbackCanvas) return;
    const source = mainCharts[param];
    if (!source) return;
    // 모달 HOLD 토글을 메인 HOLD 토글과 동기화
    if (modalHoldToggle) {
        modalHoldToggle.checked = chartHoldEnabled;
    }
    modal.style.display = 'flex'; // 먼저 열어 컨테이너 크기 확보
    renderModalChart(param, kind, true);
}

function closeChartModal() {
    const modal = document.getElementById('chart-modal');
    if (modal) modal.style.display = 'none';
    if (modalChart) {
        modalChart.clear();
        modalChart = null;
    }
    if (modalChartFallback) {
        modalChartFallback.destroy();
        modalChartFallback = null;
    }
    modalInfo = null;
    modalFrozen = false;
}

function openWarningModal(param, text) {
    if (!warningEnabled) {
        console.log(`[경고 팝업 스킵] 경고 기능이 비활성화되어 있습니다.`);
        return;
    }
    if (!warningElements.warningModal || !warningElements.warningParam) {
        console.log(`[경고 팝업 스킵] 경고 모달 요소를 찾을 수 없습니다.`);
        return;
    }

    const now = Date.now();
    const timeSinceStartup = now - pageStartTime;
    const timeSinceLastWarning = now - lastWarningTime;

    console.log(`[경고 팝업 체크] 파라미터: ${param}, 시작 후: ${Math.floor(timeSinceStartup / 1000)}초, 마지막 경고 후: ${Math.floor(timeSinceLastWarning / 1000)}초`);

    // 시작 후 5초 그레이스 기간 체크
    if (timeSinceStartup < STARTUP_GRACE_PERIOD_MS) {
        console.log(`[경고 팝업 스킵] 시작 후 ${Math.floor(timeSinceStartup / 1000)}초 (그레이스 기간 중)`);
        return; // 시작 후 5초 이내면 팝업을 표시하지 않음
    }

    // 1분 쿨다운 확인
    if (timeSinceLastWarning < WARNING_COOLDOWN_MS) {
        console.log(`[경고 팝업 스킵] 마지막 경고 후 ${Math.floor(timeSinceLastWarning / 1000)}초 (쿨다운 중)`);
        return; // 쿨다운 중이면 팝업을 표시하지 않음
    }

    console.log(`[경고 팝업 표시] 파라미터: ${param}`);
    warningElements.warningParam.textContent = text || `${displayParam(param)} 이상 감지`;
    warningElements.warningModal.classList.add('show');
    warningModalOpen = true;
    lastWarningTime = now; // 경고 팝업이 뜬 시간 기록
}

function closeWarningModal() {
    if (warningElements.warningModal) warningElements.warningModal.classList.remove('show');
    warningModalOpen = false;
}

async function openConfirmModal() {
    // 먼저 프로세스 상태 확인
    try {
        const res = await fetch('/api/equipment/status', {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });
        const data = await res.json();
        if (res.ok && data.status === 'stopped') {
            // 이미 중지된 상태면 확인 모달을 열지 않고 바로 안내 메시지 표시
            alert('⚠️ 프로세스가 이미 중지되어 있습니다.');
            closeWarningModal();
            // 텔레그램 알림 전송
            await sendTelegramNotification('⚠️ 장비가 이미 중지되어 있습니다.\nDOWN 버튼이 클릭되었지만 실행 중인 프로세스가 없습니다.');
            return;
        }
    } catch (e) {
        console.error('Error checking equipment status:', e);
        // 에러가 발생해도 확인 모달은 열도록 진행
    }

    // 프로세스가 실행 중이면 확인 모달 열기
    closeWarningModal();
    if (warningElements.confirmModal) warningElements.confirmModal.classList.add('show');
}

function closeConfirmModal() {
    if (warningElements.confirmModal) warningElements.confirmModal.classList.remove('show');
}

function buildSeverity(diff) {
    if (!Number.isFinite(diff)) return 'Level 1';
    if (diff >= 80) return 'Level 5';
    if (diff >= 60) return 'Level 4';
    if (diff >= 40) return 'Level 3';
    if (diff >= 20) return 'Level 2';
    return 'Level 1';
}

function destroyReportChart() {
    if (reportChart) {
        reportChart.destroy();
        reportChart = null;
    }
}

async function loadHtml(path, cacheKey) {
    const key = cacheKey || path;
    if (htmlCache.has(key)) return htmlCache.get(key);
    const res = await fetch(path);
    if (!res.ok) throw new Error('failed to fetch html');
    let text = await res.text();
    text = text.replace(/\.\.\/img\//g, '/static/img/'); // 이미지 경로 보정
    // htmls 폴더 내의 images 경로를 절대 경로로 변경
    const htmlsMatch = path.match(/\/static\/htmls\/([^\/]+)\/index\.html/);
    if (htmlsMatch) {
        const dir = htmlsMatch[1];
        text = text.replace(/images\//g, `/static/htmls/${dir}/images/`);
        // CSS 파일 경로 변환 (href="파일명.css")
        text = text.replace(/href=(['"])([^'"]+\.css)\1/g, (match, quote, filename) => {
            // 이미 절대 경로인 경우는 제외
            if (filename.startsWith('/') || filename.startsWith('http')) return match;
            return `href=${quote}/static/htmls/${dir}/${filename}${quote}`;
        });
        // 상대 경로 이미지 파일들을 절대 경로로 변환
        // url('파일명.png') 또는 url("파일명.png") 패턴 처리 (단, url(#...) 같은 패턴 ID는 제외)
        text = text.replace(/url\((['"]?)([^'")#]+\.(png|jpg|jpeg|gif|svg))\1\)/g, (match, quote, filename) => {
            // 이미 절대 경로인 경우는 제외
            if (filename.startsWith('/') || filename.startsWith('http')) return match;
            return `url(${quote}/static/htmls/${dir}/${filename}${quote})`;
        });
        // xlink:href="파일명.png" 또는 xlink:href='파일명.png' 패턴 처리
        text = text.replace(/xlink:href=(['"])([^'"]+\.(png|jpg|jpeg|gif|svg))\1/g, (match, quote, filename) => {
            // 이미 절대 경로인 경우는 제외
            if (filename.startsWith('/') || filename.startsWith('http')) return match;
            return `xlink:href=${quote}/static/htmls/${dir}/${filename}${quote}`;
        });
    }
    htmlCache.set(key, text);
    return text;
}

function renderHtml(htmlContainer, htmlText) {
    if (!htmlContainer) return;
    htmlContainer.innerHTML = htmlText;
    // HTML 내용 중앙 정렬을 위한 스타일 추가
    const style = document.createElement('style');
    style.textContent = `
        #mfc-md-container p {
            text-align: center;
            margin: 8px 0;
        }
        #mfc-md-container img {
            display: block;
            margin: 10px auto;
            max-width: 100%;
            height: auto;
        }
    `;
    if (!document.head.querySelector('style[data-action-center]')) {
        style.setAttribute('data-action-center', 'true');
        document.head.appendChild(style);
    }
}

function loadScriptOnce(src) {
    return new Promise((resolve, reject) => {
        const existing = document.querySelector(`script[data-dynamic-src="${src}"]`);
        if (existing) {
            if (existing.dataset.loaded === 'true') {
                resolve();
            } else if (existing.dataset.loaded === 'error') {
                existing.remove();
                resolve(loadScriptOnce(src));
            } else {
                existing.addEventListener('load', resolve, { once: true });
                existing.addEventListener('error', () => reject(new Error(`failed to load ${src}`)), { once: true });
            }
            return;
        }
        const script = document.createElement('script');
        script.src = src;
        script.async = true;
        script.dataset.dynamicSrc = src;
        script.addEventListener('load', () => {
            script.dataset.loaded = 'true';
            resolve();
        }, { once: true });
        script.addEventListener('error', () => {
            script.dataset.loaded = 'error';
            reject(new Error(`failed to load ${src}`));
        }, { once: true });
        document.head.appendChild(script);
    });
}

function disposeThreeViewer() {
    if (!threeViewer) return;
    cancelAnimationFrame(threeViewer.rafId);
    if (threeViewer.cleanup) threeViewer.cleanup();
    if (threeViewer.renderer) threeViewer.renderer.dispose();
    threeViewer = null;
}

async function loadStlGeometry(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error('failed to fetch stl');
    const buffer = await res.arrayBuffer();
    const dv = new DataView(buffer);
    const faceCount = buffer.byteLength >= 84 ? dv.getUint32(80, true) : 0;
    const expectedLength = 84 + faceCount * 50;
    const headText = new TextDecoder().decode(buffer.slice(0, 80));
    const looksAsciiHeader = headText.trim().startsWith('solid');
    const isLikelyBinary = expectedLength === buffer.byteLength;

    // Binary STL
    if (isLikelyBinary) {
        const faceLimit = 1500000; // 최대 150만 면까지 허용
        if (faceCount > faceLimit) {
            throw new Error(`STL faces too large (${faceCount})`);
        }
        const positions = new Float32Array(faceCount * 9);
        const normals = new Float32Array(faceCount * 9);
        let offset = 84;
        for (let i = 0; i < faceCount; i++) {
            const nx = dv.getFloat32(offset, true);
            const ny = dv.getFloat32(offset + 4, true);
            const nz = dv.getFloat32(offset + 8, true);
            offset += 12;
            for (let v = 0; v < 3; v++) {
                const vx = dv.getFloat32(offset, true);
                const vy = dv.getFloat32(offset + 4, true);
                const vz = dv.getFloat32(offset + 8, true);
                const idx = i * 9 + v * 3;
                positions[idx] = vx; positions[idx + 1] = vy; positions[idx + 2] = vz;
                normals[idx] = nx; normals[idx + 1] = ny; normals[idx + 2] = nz;
                offset += 12;
            }
            offset += 2; // attr byte count
        }
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        geometry.setAttribute('normal', new THREE.BufferAttribute(normals, 3));
        geometry.computeBoundingSphere();
        return geometry;
    }

    // ASCII STL fallback
    if (looksAsciiHeader) {
        const text = new TextDecoder().decode(buffer);
        const vertexPattern = /vertex\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s+([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)/g;
        const vertices = [];
        let m;
        while ((m = vertexPattern.exec(text)) !== null) {
            vertices.push(parseFloat(m[1]), parseFloat(m[2]), parseFloat(m[3]));
        }
        if (vertices.length / 3 > 5_000_000) {
            throw new Error('STL vertices too large');
        }
        const geometry = new THREE.BufferGeometry();
        geometry.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
        geometry.computeVertexNormals();
        return geometry;
    }
    throw new Error('Unsupported STL format');
}

async function ensureGltfLoader() {
    if (THREE.GLTFLoader) return;
    const localSrc = '/static/lib/GLTFLoader.js';
    const cdnVersion = '0.150.0';
    const cdnSrc = `https://cdn.jsdelivr.net/npm/three@${cdnVersion}/examples/js/loaders/GLTFLoader.js`;
    let lastErr = null;
    try {
        await loadScriptOnce(localSrc);
    } catch (err) {
        lastErr = err;
    }
    if (!THREE.GLTFLoader) {
        try {
            await loadScriptOnce(cdnSrc);
        } catch (err) {
            lastErr = err;
        }
    }
    if (!THREE.GLTFLoader) {
        throw new Error(`GLTFLoader unavailable (local/CDN load failed${lastErr ? ': ' + lastErr.message : ''})`);
    }
}

async function ensureDracoLoader() {
    if (THREE.DRACOLoader) return;

    // 로컬 파일 경로 먼저 시도
    const localSrc = '/static/lib/DRACOLoader.js';
    const cdnVersion = '0.150.0';

    // 로컬 파일 시도
    try {
        await loadScriptOnce(localSrc);
        if (THREE.DRACOLoader) {
            console.log('DRACOLoader loaded from local file');
            return;
        }
    } catch (err) {
        console.warn('Local DRACOLoader not found, trying CDN...', err);
    }

    // CDN 경로 시도 (여러 옵션)
    const cdnPaths = [
        `https://cdn.jsdelivr.net/npm/three@${cdnVersion}/examples/js/loaders/DRACOLoader.js`,
        `https://unpkg.com/three@${cdnVersion}/examples/js/loaders/DRACOLoader.js`,
        `https://cdn.jsdelivr.net/npm/three@${cdnVersion}/examples/jsm/loaders/DRACOLoader.js`
    ];

    let lastErr = null;
    for (const cdnSrc of cdnPaths) {
        try {
            await loadScriptOnce(cdnSrc);
            if (THREE.DRACOLoader) {
                console.log('DRACOLoader loaded from CDN:', cdnSrc);
                return;
            }
        } catch (err) {
            lastErr = err;
            console.warn('Failed to load DRACOLoader from:', cdnSrc, err);
        }
    }

    if (!THREE.DRACOLoader) {
        // 사용자에게 명확한 안내 메시지 제공
        const downloadUrl = `https://cdn.jsdelivr.net/npm/three@${cdnVersion}/examples/js/loaders/DRACOLoader.js`;
        const errorMsg = `DRACOLoader를 로드할 수 없습니다.

해결 방법:
1. 다음 명령어를 실행하여 DRACOLoader.js를 다운로드하세요:
   curl -L -o /home/goo4168/semi_platform/semi_ondevice/fastapi/static/lib/DRACOLoader.js "${downloadUrl}"

2. 또는 브라우저에서 다음 URL을 열어 파일을 저장하세요:
   ${downloadUrl}
   저장 위치: /static/lib/DRACOLoader.js

마지막 오류: ${lastErr ? lastErr.message : 'unknown'}`;
        console.error(errorMsg);
        throw new Error(errorMsg);
    }
}

async function loadGltfModel(url) {
    await ensureGltfLoader();
    await ensureDracoLoader();

    if (!THREE.DRACOLoader) {
        throw new Error('DRACOLoader is required but not available');
    }

    return new Promise((resolve, reject) => {
        const loader = new THREE.GLTFLoader();

        // DRACOLoader 인스턴스 생성 및 설정
        try {
            const dracoLoader = new THREE.DRACOLoader();

            // DRACO 디코더 경로 설정 (여러 옵션 시도)
            const decoderPaths = [
                'https://www.gstatic.com/draco/v1/decoders/',
                'https://cdn.jsdelivr.net/npm/three@0.150.0/examples/jsm/libs/draco/gltf/',
                'https://unpkg.com/three@0.150.0/examples/jsm/libs/draco/gltf/'
            ];

            if (typeof dracoLoader.setDecoderPath === 'function') {
                // 첫 번째 경로 사용 (Google 공식)
                dracoLoader.setDecoderPath(decoderPaths[0]);
            } else {
                console.warn('DRACOLoader.setDecoderPath is not a function');
            }

            // GLTFLoader에 DRACOLoader 설정
            loader.setDRACOLoader(dracoLoader);
            console.log('DRACOLoader configured for GLTFLoader');
        } catch (e) {
            console.error('Failed to configure DRACOLoader:', e);
            reject(new Error(`Failed to configure DRACOLoader: ${e.message}`));
            return;
        }

        loader.load(
            url,
            (gltf) => {
                const root = gltf.scene || gltf.scenes?.[0];
                if (!root) {
                    reject(new Error('empty gltf scene'));
                    return;
                }
                root.updateMatrixWorld(true);
                const box = new THREE.Box3().setFromObject(root);
                resolve({ object: root, box });
            },
            undefined,
            (err) => {
                console.error('GLTF load error:', err);
                reject(err || new Error('failed to load gltf'));
            }
        );
    });
}

function attachOrbitControls(canvas, camera, target, opts = {}) {
    const minRadius = opts.minRadius ?? 0.1;
    const maxRadius = opts.maxRadius ?? 5000;
    let isRotating = false;
    let lastX = 0, lastY = 0;
    // target을 Vector3 객체로 유지 (참조로 전달)
    const targetVec = target instanceof THREE.Vector3 ? target : new THREE.Vector3().copy(target);
    // 현재 카메라 위치를 기준으로 초기화
    let radius = camera.position.clone().sub(targetVec).length();
    let spherical = new THREE.Spherical().setFromVector3(camera.position.clone().sub(targetVec));
    let enabled = true; // OrbitControls 활성화 상태

    const updateSpherical = () => {
        radius = camera.position.clone().sub(targetVec).length();
        spherical = new THREE.Spherical().setFromVector3(camera.position.clone().sub(targetVec));
    };

    // 초기화 시 spherical 업데이트
    updateSpherical();

    const onPointerDown = (e) => {
        if (e.button !== 0 || !enabled) {
            console.log('onPointerDown 무시:', { button: e.button, enabled });
            return;
        }
        e.preventDefault();
        e.stopPropagation();
        if (e.pointerId !== undefined) {
            try {
                canvas.setPointerCapture(e.pointerId);
            } catch (err) {
                console.warn('setPointerCapture 실패:', err);
            }
        }
        isRotating = true;
        lastX = e.clientX; lastY = e.clientY;
        // 드래그 시작 시 현재 위치 기준으로 spherical 업데이트
        updateSpherical();
        console.log('드래그 시작', { enabled, isRotating, cameraPos: camera.position, target: targetVec });
    };
    const onPointerMove = (e) => {
        if (!isRotating || !enabled) return;
        e.preventDefault();
        const dx = e.clientX - lastX;
        const dy = e.clientY - lastY;
        lastX = e.clientX; lastY = e.clientY;
        const ROT_SPEED = 0.005;
        spherical.theta -= dx * ROT_SPEED;
        spherical.phi -= dy * ROT_SPEED;
        const EPS = 0.0001;
        spherical.phi = Math.max(EPS, Math.min(Math.PI - EPS, spherical.phi));
        const vec = new THREE.Vector3().setFromSpherical(spherical).add(targetVec);
        camera.position.copy(vec);
        camera.lookAt(targetVec);
    };
    const onPointerUp = (e) => {
        if (e.pointerId !== undefined) {
            try {
                canvas.releasePointerCapture(e.pointerId);
            } catch (err) {
                console.warn('releasePointerCapture 실패:', err);
            }
        }
        isRotating = false;
    };
    const onPointerCancel = (e) => {
        if (e.pointerId !== undefined) {
            try {
                canvas.releasePointerCapture(e.pointerId);
            } catch (err) {
                console.warn('releasePointerCapture 실패:', err);
            }
        }
        isRotating = false;
    };
    const onWheel = (e) => {
        if (!enabled) {
            console.log('onWheel 무시:', { enabled });
            return;
        }
        e.preventDefault();
        e.stopPropagation();
        // 줌 전에 현재 위치 기준으로 spherical 업데이트
        updateSpherical();
        const delta = e.deltaY * 0.001;
        const zoomFactor = Math.exp(delta); // smoother zoom curve
        radius = Math.min(maxRadius, Math.max(minRadius, radius * zoomFactor));
        spherical.radius = radius;
        const vec = new THREE.Vector3().setFromSpherical(spherical).add(targetVec);
        camera.position.copy(vec);
        camera.lookAt(targetVec);
        console.log('줌 실행', { enabled, radius });
    };
    // 이벤트 리스너 등록
    canvas.addEventListener('pointerdown', onPointerDown, { passive: false });
    window.addEventListener('pointermove', onPointerMove, { passive: false });
    window.addEventListener('pointerup', onPointerUp, { passive: false });
    canvas.addEventListener('pointercancel', onPointerCancel, { passive: false });
    canvas.addEventListener('wheel', onWheel, { passive: false });

    console.log('OrbitControls 이벤트 리스너 등록 완료', {
        canvas: canvas,
        hasPointerDown: true,
        hasPointerMove: true,
        hasWheel: true
    });

    // 활성화/비활성화 함수 반환
    const setEnabled = (value) => {
        const oldValue = enabled;
        enabled = value;
        console.log('OrbitControls setEnabled:', { old: oldValue, new: value });
        if (!value) {
            isRotating = false; // 비활성화 시 회전 중지
        } else {
            // 활성화 시 현재 카메라 위치 기준으로 spherical 업데이트
            updateSpherical();
        }
    };

    // 타겟 업데이트 함수
    const updateTarget = (newTarget) => {
        targetVec.copy(newTarget);
        // 현재 카메라 위치를 기준으로 spherical과 radius 재계산
        updateSpherical();
    };

    return {
        cleanup: () => {
            canvas.removeEventListener('pointerdown', onPointerDown);
            window.removeEventListener('pointermove', onPointerMove);
            window.removeEventListener('pointerup', onPointerUp);
            canvas.removeEventListener('pointercancel', onPointerCancel);
            canvas.removeEventListener('wheel', onWheel);
        },
        setEnabled: setEnabled,
        updateTarget: updateTarget
    };
}

// 메쉬를 이름으로 찾는 함수
function findMeshByName(root, name) {
    let found = null;
    root.traverse((child) => {
        if (child.name === name && child instanceof THREE.Mesh) {
            found = child;
        }
    });
    return found;
}

// 모든 wafer-jig 메쉬 찾기 (xxTJIG01 패턴)
function findWaferJigMeshes(root) {
    const meshes = [];
    root.traverse((child) => {
        if (child.name && child.name.includes('xxTJIG01') && child instanceof THREE.Mesh) {
            meshes.push(child);
        }
    });
    return meshes;
}

async function createThreeViewer(container, modelUrl, entry = null) {
    if (!window.THREE || !container) {
        container.textContent = '3D 라이브러리를 불러오지 못했습니다.';
        return;
    }
    disposeThreeViewer();
    const width = container.clientWidth || 600;
    // 컨테이너의 실제 높이를 사용하거나, 최소 높이 보장
    const height = Math.max(container.clientHeight || 420, 420);
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(window.devicePixelRatio || 1);
    renderer.setSize(width, height);
    renderer.setClearColor(0xededf1, 1);
    renderer.outputEncoding = THREE.sRGBEncoding;
    renderer.toneMappingExposure = 1.45;
    container.innerHTML = '';
    container.appendChild(renderer.domElement);

    // 컨테이너 크기 변경 감지 및 렌더러 크기 조정
    const resizeObserver = new ResizeObserver(() => {
        const newWidth = container.clientWidth || 600;
        const newHeight = Math.max(container.clientHeight || 420, 400);
        if (renderer.domElement.width !== newWidth || renderer.domElement.height !== newHeight) {
            renderer.setSize(newWidth, newHeight);
            camera.aspect = newWidth / newHeight;
            camera.updateProjectionMatrix();
        }
    });
    resizeObserver.observe(container);

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0xededf1);
    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 5000);
    camera.position.set(0, 0, 200);

    scene.add(new THREE.AmbientLight(0xffffff, 1.2));
    scene.add(new THREE.HemisphereLight(0xffffff, 0xdde3ee, 1.0));
    const dir = new THREE.DirectionalLight(0xffffff, 1.8);
    dir.position.set(0, 120, 220); // 메인 라이트를 정면 위쪽에서 비추도록 조정
    scene.add(dir);
    const dirFill = new THREE.DirectionalLight(0xf7f7f7, 0.85);
    dirFill.position.set(30, 50, 180); // 정면 보조광
    scene.add(dirFill);
    const rim = new THREE.DirectionalLight(0xf0f0f0, 0.4);
    rim.position.set(0, 140, -120);
    scene.add(rim);
    const frontFill = new THREE.PointLight(0xffffff, 0.6, 2000);
    frontFill.position.set(0, 40, 260);
    scene.add(frontFill);

    let cleanup = null;
    const modelGroup = new THREE.Group();
    scene.add(modelGroup);

    // 애니메이션 관련 변수
    let highlightMesh = null;
    let highlightStartTime = 0;
    let highlightConfig = null;
    let targetMesh = null;
    let originalMaterialState = null;
    let cameraAnimationStartTime = 0;
    let initialCameraPosition = null;
    let initialCameraTarget = null;
    let finalCameraPosition = null;
    let finalCameraTarget = null;
    let isCameraAnimating = false;
    let orbitTarget = null; // OrbitControls의 타겟
    let orbitControlsEnabled = true; // OrbitControls 활성화 여부
    let orbitControls = null; // OrbitControls 객체 참조

    try {
        const lower = (modelUrl || '').toLowerCase();
        let box = null;
        let target = new THREE.Vector3();
        let size = 100;
        if (lower.endsWith('.stl')) {
            const geometry = await loadStlGeometry(modelUrl);
            geometry.center();
            const material = new THREE.MeshPhongMaterial({
                color: 0xf8f8f8,
                specular: 0x999999,
                shininess: 45,
                emissive: 0x121212
            });
            const mesh = new THREE.Mesh(geometry, material);
            modelGroup.add(mesh);
            box = new THREE.Box3().setFromObject(modelGroup);
        } else if (lower.endsWith('.glb') || lower.endsWith('.gltf')) {
            const { object, box: gltfBox } = await loadGltfModel(modelUrl);
            modelGroup.add(object);
            box = new THREE.Box3().setFromObject(modelGroup);

            // violation_type에 따라 애니메이션 설정
            const violationType = Number(entry?.violation_type) || 0;
            let meshName = null;
            let meshColor = null;
            let emissiveIntensityRange = [2.0, 4.0];
            let yawOffset = 0;

            if (violationType === 1) {
                // BG (Baratron Gauge)
                meshName = 'bg';
                meshColor = new THREE.Color(0.1, 0.3, 5.0); // 파란색
                emissiveIntensityRange = [2.5, 5.0];
                yawOffset = Math.PI * 160 / 180; // 약 160도
            } else if (violationType === 2) {
                // MFC
                meshName = 'mfc';
                meshColor = new THREE.Color(1.5, 0.05, 0.05); // 붉은색
                emissiveIntensityRange = [2.0, 4.0];
                yawOffset = Math.PI / 1.6; // 약 120도
            } else if (violationType === 3) {
                // wafer-jig (CKD)
                meshName = 'xxTJIG01-FMS-4000-R00'; // 첫 번째 Jig 메쉬 이름
                meshColor = new THREE.Color(0.8, 0.8, 0.0); // 노란색
                emissiveIntensityRange = [2.0, 4.0];
                yawOffset = Math.PI / 2; // 90도
            } else if (violationType === 4) {
                // CKD
                meshName = 'rear_L001';
                meshColor = new THREE.Color(0.0, 1.5, 0.0); // 초록색
                emissiveIntensityRange = [2.0, 4.0];
                yawOffset = Math.PI * 200 / 180; // 200도
            }

            // 메쉬 찾기 및 하이라이트 설정
            if (meshName) {
                if (violationType === 3) {
                    // wafer-jig는 여러 메쉬를 찾아야 함
                    const waferJigMeshes = findWaferJigMeshes(object);
                    if (waferJigMeshes.length > 0) {
                        highlightMesh = waferJigMeshes[0];
                        // 모든 wafer-jig 메쉬를 userData에 저장
                        highlightMesh.userData.allWaferJigMeshes = waferJigMeshes;
                    }
                } else {
                    highlightMesh = findMeshByName(object, meshName);
                }

                if (highlightMesh) {
                    highlightConfig = {
                        color: meshColor,
                        emissiveIntensityRange: emissiveIntensityRange,
                        yawOffset: yawOffset
                    };
                    // 하이라이트 애니메이션은 카메라 애니메이션 완료 후 시작 (2.5초 후)
                    highlightStartTime = performance.now() + 2500;

                    // 원본 재질 상태 저장
                    const materials = Array.isArray(highlightMesh.material) ? highlightMesh.material : [highlightMesh.material];
                    originalMaterialState = materials.map((mat) => ({
                        color: mat.color ? mat.color.clone() : null,
                        emissive: mat.emissive ? mat.emissive.clone() : null,
                        emissiveIntensity: mat.emissiveIntensity ?? 1,
                    }));

                    // 카메라 포커스 설정
                    const meshBox = new THREE.Box3().setFromObject(highlightMesh);
                    const meshSphere = meshBox.getBoundingSphere(new THREE.Sphere());
                    if (meshSphere) {
                        targetMesh = {
                            center: meshSphere.center.clone(),
                            radius: meshSphere.radius || 1
                        };
                    }
                }
            }
        } else {
            throw new Error('Unsupported model type');
        }
        // 초기 회전 제거 - 정면이 바로 보이도록
        modelGroup.rotation.y = 0;
        size = box?.getSize(new THREE.Vector3()).length() || 100;
        target = box?.getCenter(new THREE.Vector3()) || new THREE.Vector3();
        // 회전축을 아래로 조정 (y 값을 줄여서 회전 중심을 아래로 이동)
        target.y -= size * 0.2;

        // 초기 시야를 정면-약간 위쪽에서 바라보도록 설정 (항상 정면부터 시작)
        const frontDistance = size * 1.1;
        initialCameraPosition = new THREE.Vector3(target.x, target.y + size * 0.12, target.z + frontDistance);
        initialCameraTarget = target.clone();
        camera.position.copy(initialCameraPosition);
        camera.lookAt(initialCameraTarget);

        // 타겟 메쉬가 있으면 최종 카메라 위치 계산 (애니메이션용)
        if (targetMesh && highlightConfig) {
            // violation_type == 3 (Magnetic Seal)인 경우 확대를 더 해줌
            const violationType = Number(entry?.violation_type) || 0;
            const focusDistance = violationType === 3
                ? targetMesh.radius * 1.8  // Magnetic Seal은 더 가깝게 (확대)
                : targetMesh.radius * 2.5; // 나머지는 기본값
            const spherical = new THREE.Spherical();
            spherical.radius = focusDistance;
            spherical.theta = highlightConfig.yawOffset || 0;
            spherical.phi = Math.PI / 3; // 약 60도 위에서
            finalCameraPosition = new THREE.Vector3().setFromSpherical(spherical).add(targetMesh.center);
            finalCameraTarget = targetMesh.center.clone();
            // 카메라 애니메이션 시작 (1초 후)
            cameraAnimationStartTime = performance.now() + 1000; // 1초 후 시작
            isCameraAnimating = true;
            // 애니메이션 시작 전에 OrbitControls 비활성화
            if (orbitControls) {
                orbitControls.setEnabled(false);
                orbitControlsEnabled = false;
                console.log('OrbitControls 비활성화 (애니메이션 시작)');
            }
            target = targetMesh.center.clone();
        } else {
            finalCameraPosition = null;
            finalCameraTarget = null;
            isCameraAnimating = false;
        }

        camera.near = size / 200;
        camera.far = size * 20;
        camera.updateProjectionMatrix();
        camera.lookAt(target);
        const minRadius = Math.max(0.1, size * 0.01); // 더 깊게 확대 허용
        const maxRadius = size * 50;
        orbitTarget = target.clone(); // OrbitControls 타겟 초기화

        // violation_type이 없거나 애니메이션이 없는 경우 즉시 OrbitControls 생성
        if (!targetMesh || !highlightConfig) {
            orbitControls = attachOrbitControls(renderer.domElement, camera, orbitTarget, { minRadius, maxRadius });
            cleanup = orbitControls.cleanup;
            orbitControlsEnabled = true;
            orbitControls.setEnabled(true);
            console.log('OrbitControls 초기화 완료 (애니메이션 없음)');
        } else {
            // 애니메이션이 있는 경우에도 초기 OrbitControls 생성 (애니메이션 시작 전까지 사용)
            orbitControls = attachOrbitControls(renderer.domElement, camera, orbitTarget, { minRadius, maxRadius });
            cleanup = orbitControls.cleanup;
            orbitControlsEnabled = true;
            orbitControls.setEnabled(true);
            console.log('OrbitControls 초기화 완료 (애니메이션 대기)');
        }
    } catch (err) {
        console.error('3D load failed', err);
        const msg = String(err?.message || '');
        const tooLarge = msg.includes('too large') || msg.includes('Exceeded');
        const loaderMissing = msg.includes('GLTFLoader unavailable');
        if (loaderMissing) {
            container.innerHTML = 'GLTFLoader를 불러오지 못했습니다. /static/lib/GLTFLoader.js를 배포하거나 네트워크 접속을 허용해 주세요.';
        } else if (tooLarge) {
            container.innerHTML = '모델이 너무 커서 표시할 수 없습니다. 용량을 줄인 모델을 제공해 주세요.';
        } else {
            container.innerHTML = `3D 모델을 불러오지 못했습니다. (${msg || '원인 미상'})`;
        }
        return;
    }

    function animate() {
        const currentTime = performance.now();

        // 카메라 애니메이션 (정면 → 이상위치로 이동)
        if (isCameraAnimating && finalCameraPosition && finalCameraTarget) {
            const elapsed = (currentTime - cameraAnimationStartTime) / 1000;
            const duration = 1.5; // 1.5초 동안 이동

            // 애니메이션 중에는 OrbitControls 비활성화
            if (orbitControls && orbitControlsEnabled) {
                orbitControls.setEnabled(false);
                orbitControlsEnabled = false;
            }

            if (elapsed < duration && elapsed > 0) {
                // 부드러운 이징 함수 (ease-in-out)
                const t = elapsed / duration;
                const eased = t < 0.5
                    ? 2 * t * t
                    : 1 - Math.pow(-2 * t + 2, 2) / 2;

                // 카메라 위치 보간
                camera.position.lerpVectors(initialCameraPosition, finalCameraPosition, eased);

                // 카메라 타겟 보간
                const currentTarget = new THREE.Vector3().lerpVectors(initialCameraTarget, finalCameraTarget, eased);
                camera.lookAt(currentTarget);
            } else if (elapsed >= duration) {
                // 애니메이션 완료
                camera.position.copy(finalCameraPosition);
                camera.lookAt(finalCameraTarget);
                isCameraAnimating = false;
                // OrbitControls 타겟을 최종 타겟으로 업데이트
                orbitTarget.copy(finalCameraTarget);

                // OrbitControls가 이미 있으면 타겟만 업데이트하고 활성화
                if (orbitControls) {
                    orbitControls.updateTarget(orbitTarget);
                    orbitControlsEnabled = true;
                    orbitControls.setEnabled(true);
                    console.log('OrbitControls 타겟 업데이트 및 활성화 완료', {
                        cameraPosition: camera.position,
                        target: orbitTarget,
                        enabled: orbitControlsEnabled
                    });
                } else {
                    // OrbitControls가 없으면 새로 생성
                    if (cleanup) {
                        cleanup();
                        console.log('기존 OrbitControls 정리 완료');
                    }
                    orbitControls = attachOrbitControls(renderer.domElement, camera, orbitTarget, {
                        minRadius: Math.max(0.1, size * 0.01),
                        maxRadius: size * 50
                    });
                    cleanup = orbitControls.cleanup;
                    orbitControlsEnabled = true;
                    orbitControls.setEnabled(true);
                    console.log('OrbitControls 새로 생성 및 활성화 완료', {
                        cameraPosition: camera.position,
                        target: orbitTarget,
                        enabled: orbitControlsEnabled
                    });
                }
            }
        }

        // 하이라이트 애니메이션 (카메라 애니메이션 완료 후 시작)
        if (highlightMesh && highlightConfig && originalMaterialState) {
            const highlightElapsed = (currentTime - highlightStartTime) / 1000;
            if (highlightElapsed > 0) {
                const pulse = 0.5 + 0.5 * Math.sin(highlightElapsed * 4); // 0~1 사이 펄스
                const materials = Array.isArray(highlightMesh.material) ? highlightMesh.material : [highlightMesh.material];

                materials.forEach((mat, idx) => {
                    if (!mat) return;
                    const orig = originalMaterialState[idx];
                    if (!orig) return;

                    // emissive 색상 설정
                    if (mat.emissive) {
                        mat.emissive.setRGB(
                            highlightConfig.color.r,
                            highlightConfig.color.g,
                            highlightConfig.color.b
                        );
                    }

                    // emissive 강도 애니메이션
                    if (mat.emissiveIntensity !== undefined) {
                        const [minI, maxI] = highlightConfig.emissiveIntensityRange;
                        mat.emissiveIntensity = minI + pulse * (maxI - minI);
                    }
                });

                // wafer-jig의 경우 모든 메쉬에 적용
                if (highlightMesh.userData && highlightMesh.userData.allWaferJigMeshes) {
                    highlightMesh.userData.allWaferJigMeshes.forEach((jigMesh) => {
                        if (jigMesh === highlightMesh) return; // 이미 처리됨
                        const jigMaterials = Array.isArray(jigMesh.material) ? jigMesh.material : [jigMesh.material];
                        jigMaterials.forEach((mat) => {
                            if (!mat) return;
                            if (mat.emissive) {
                                mat.emissive.setRGB(
                                    highlightConfig.color.r,
                                    highlightConfig.color.g,
                                    highlightConfig.color.b
                                );
                            }
                            if (mat.emissiveIntensity !== undefined) {
                                const [minI, maxI] = highlightConfig.emissiveIntensityRange;
                                mat.emissiveIntensity = minI + pulse * (maxI - minI);
                            }
                        });
                    });
                }
            }
        }

        renderer.render(scene, camera);
        threeViewer.rafId = requestAnimationFrame(animate);
    }

    // cleanup 함수에 resizeObserver 정리 추가
    const originalCleanup = cleanup;
    const enhancedCleanup = () => {
        if (originalCleanup) originalCleanup();
        if (resizeObserver) resizeObserver.disconnect();
    };

    threeViewer = { renderer, scene, camera, modelGroup, rafId: requestAnimationFrame(animate), cleanup: enhancedCleanup };
}

function buildCauseHeadline(entry) {
    const diffRaw = entry.diff != null ? Math.abs(entry.diff) : 0;
    const diff = (diffRaw / 10).toFixed(1);
    const direction = (entry.actual_value ?? 0) - (entry.predicted_value ?? 0);
    if (direction > 0.0001) return `유량 +${diff}% 변화 상승 감지`;
    if (direction < -0.0001) return `유량 -${diff}% 변화 하강 감지`;
    return `유량 편차 ${diff}% 감지`;
}

function replaceCanvasWithMessage(canvas, text) {
    if (!canvas) return;
    const msg = document.createElement('div');
    msg.className = 'report-cause-lines';
    msg.textContent = text;
    canvas.replaceWith(msg);
}

async function drawReportChart(entry, canvas, timeWindow) {
    if (!canvas) return;
    destroyReportChart();
    const ctx = canvas.getContext('2d');
    // 차트 높이를 창 크기에 맞춤
    const chartContainer = canvas.closest('.report-cause-chart');
    if (chartContainer) {
        const containerHeight = chartContainer.clientHeight - 60; // 제목 등 여유 공간
        canvas.style.height = Math.max(300, containerHeight) + 'px';
        canvas.height = Math.max(300, containerHeight);
    } else {
        canvas.style.height = '300px';
        canvas.height = 300;
    }
    const centerRaw = entry.peak_time ?? entry.end ?? entry.start ?? Date.now();
    const center = toMillis(centerRaw) ?? Date.now();
    const windowMs = timeWindow?.windowMs ?? 15000; // 넉넉히 ±15초 범위
    const startMs = timeWindow?.startMs ?? (center - windowMs);
    const endMs = timeWindow?.endMs ?? (center + windowMs);
    const startIso = new Date(startMs).toISOString();
    const endIso = new Date(endMs).toISOString();

    // limits.yaml에서 상한값/하한값 가져오기
    let limits = {};
    try {
        const limitsRes = await fetch('/api/limits');
        if (limitsRes.ok) {
            const limitsData = await limitsRes.json();
            // API 응답 구조 확인: limits 키가 있으면 그것을 사용, 없으면 전체를 limits로 사용
            limits = limitsData.limits || limitsData || {};
            console.log('Loaded limits, total keys:', Object.keys(limits).length);
            console.log('Sample limits keys:', Object.keys(limits).slice(0, 5));
        } else {
            console.error('Failed to load limits, status:', limitsRes.status);
        }
    } catch (err) {
        console.error('failed to load limits', err);
    }

    let payload;
    try {
        const res = await fetch(`/api/event_chart?param=${encodeURIComponent(entry.param)}&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(endIso)}`);
        if (!res.ok) throw new Error('bad response');
        payload = await res.json();
    } catch (err) {
        console.error('failed to load report chart', err);
        replaceCanvasWithMessage(canvas, '차트 데이터를 불러오지 못했습니다.');
        return;
    }
    const mapPoints = (arr) => (arr || []).map(pt => {
        const xRaw = pt.x ?? pt.time ?? pt.timestamp ?? pt.t;
        const yRaw = pt.y ?? pt.value ?? pt.v;
        const x = parseTimestamp(xRaw);
        if (Number.isNaN(x?.getTime())) return null;
        const y = Number(yRaw);
        if (!Number.isFinite(y)) return null;
        return { x, y };
    }).filter(Boolean);

    const actual = mapPoints(payload?.actual);
    const predicted = mapPoints(payload?.predicted);
    const regions = [];
    if (entry.start && entry.end) {
        regions.push({ start: entry.start, end: entry.end });
    }
    if (!actual.length) {
        replaceCanvasWithMessage(canvas, '해당 구간 데이터가 없습니다.');
        return;
    }

    // limits.yaml에서 상한값(UCU)과 하한값(LCL) 가져오기
    let ucu = null;
    let lcl = null;

    console.log('=== Limits Debug Start ===');
    console.log('Entry param:', entry.param);
    console.log('Entry step_id:', entry.step_id);
    console.log('Limits object keys count:', Object.keys(limits).length);

    // 파라미터명으로 limits 찾기
    let paramLimits = limits[entry.param];
    console.log('Direct match for', entry.param, ':', paramLimits ? 'FOUND' : 'NOT FOUND');

    // 정확한 매칭이 안 되면 모든 키를 확인
    if (!paramLimits) {
        const paramKeys = Object.keys(limits);
        console.log('Searching in', paramKeys.length, 'keys...');

        // 정확히 일치하는 키 찾기
        for (const key of paramKeys) {
            if (key === entry.param) {
                paramLimits = limits[key];
                console.log('Found exact match:', key);
                break;
            }
        }

        // 여전히 못 찾았으면 유사한 것 찾기
        if (!paramLimits) {
            for (const key of paramKeys) {
                const normalizedKey = key.replace(/[._-]/g, '');
                const normalizedParam = entry.param.replace(/[._-]/g, '');
                if (normalizedKey === normalizedParam) {
                    paramLimits = limits[key];
                    console.log('Found normalized match:', key, 'for param:', entry.param);
                    break;
                }
            }
        }
    }

    if (paramLimits) {
        console.log('ParamLimits found:', Object.keys(paramLimits));

        // step_id 처리
        let stepId = 'all';
        if (Array.isArray(entry.step_id) && entry.step_id.length > 0) {
            stepId = String(entry.step_id[0]);
        } else if (entry.step_id) {
            stepId = String(entry.step_id);
        }

        console.log('Using stepId:', stepId);

        // 해당 step_id의 limits 찾기, 없으면 'all' 사용
        let stepLimits = paramLimits[stepId];
        if (!stepLimits || Object.keys(stepLimits).length === 0) {
            stepLimits = paramLimits['all'];
            console.log('Using "all" limits instead of stepId:', stepId);
        }

        console.log('StepLimits:', stepLimits);

        if (stepLimits) {
            ucu = stepLimits.max;
            lcl = stepLimits.min;
            console.log('Extracted UCU:', ucu, 'LCL:', lcl);
        } else {
            console.warn('No stepLimits found for stepId:', stepId, 'or "all"');
        }
    } else {
        console.error('NO PARAMLIMITS FOUND for:', entry.param);
        console.log('Available param keys (first 20):', Object.keys(limits).slice(0, 20));
        // N2 관련 키만 필터링해서 보기
        const n2Keys = Object.keys(limits).filter(k => k.includes('N2'));
        console.log('N2 related keys:', n2Keys);
    }

    console.log('=== Limits Debug End ===');
    console.log('Final UCU:', ucu, 'LCL:', lcl);

    // 실제값만 사용하여 Y축 범위 계산
    const values = actual.map(d => d.y);
    const yMax = values.length ? Math.max(...values) : undefined;
    const yMin = values.length ? Math.min(...values) : undefined;

    // 상한값/하한값이 있으면 Y축 범위에 포함
    let finalYMax = yMax;
    let finalYMin = yMin;
    if (ucu !== null && ucu !== undefined && Number.isFinite(Number(ucu))) {
        finalYMax = finalYMax !== undefined ? Math.max(finalYMax, Number(ucu)) : Number(ucu);
    }
    if (lcl !== null && lcl !== undefined && Number.isFinite(Number(lcl))) {
        finalYMin = finalYMin !== undefined ? Math.min(finalYMin, Number(lcl)) : Number(lcl);
    }

    // 상한선/하한선 데이터 생성
    const datasets = [
        { label: '실제값', borderColor: 'blue', tension: 0.25, borderWidth: 3, pointRadius: 0, data: actual, hidden: false }
    ];

    // 상한선(UCU) - 초록색 (차트 전체에 표시)
    if (ucu !== null && ucu !== undefined && Number.isFinite(Number(ucu))) {
        const ucuValue = Number(ucu);
        // 차트 전체 시간 범위에 상한선 표시
        const chartStart = parseTimestamp(startIso);
        const chartEnd = parseTimestamp(endIso);
        const ucuLine = [
            { x: chartStart, y: ucuValue },
            { x: chartEnd, y: ucuValue }
        ];
        datasets.push({
            label: 'UCU (상한값)',
            borderColor: 'green',
            borderWidth: 2,
            borderDash: [5, 5],
            pointRadius: 0,
            data: ucuLine,
            tension: 0
        });
        console.log('Added UCU line with value:', ucuValue, 'points:', ucuLine.length);
    } else {
        console.warn('UCU not added - value:', ucu, 'isFinite:', ucu !== null && ucu !== undefined ? Number.isFinite(Number(ucu)) : false);
    }

    // 하한선(LCL) - 주황색 (차트 전체에 표시)
    if (lcl !== null && lcl !== undefined && Number.isFinite(Number(lcl))) {
        const lclValue = Number(lcl);
        // 차트 전체 시간 범위에 하한선 표시
        const chartStart = parseTimestamp(startIso);
        const chartEnd = parseTimestamp(endIso);
        const lclLine = [
            { x: chartStart, y: lclValue },
            { x: chartEnd, y: lclValue }
        ];
        datasets.push({
            label: 'LCL (하한값)',
            borderColor: 'orange',
            borderWidth: 2,
            borderDash: [5, 5],
            pointRadius: 0,
            data: lclLine,
            tension: 0
        });
        console.log('Added LCL line with value:', lclValue, 'points:', lclLine.length);
    } else {
        console.warn('LCL not added - value:', lcl, 'isFinite:', lcl !== null && lcl !== undefined ? Number.isFinite(Number(lcl)) : false);
    }

    console.log('Total datasets to render:', datasets.length, 'datasets:', datasets.map(d => d.label));

    reportChart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: datasets
        },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true },
                highlightRegion: { regions }
            },
            scales: {
                x: {
                    type: 'time',
                    min: parseTimestamp(startIso),
                    max: parseTimestamp(endIso),
                    time: {
                        unit: 'second',
                        tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                        displayFormats: {
                            second: 'HH:mm:ss',
                            minute: 'HH:mm:ss'
                        }
                    },
                    ticks: {
                        autoSkip: true,
                        maxRotation: 0,
                        callback: function (value, index, ticks) {
                            const d = new Date(value);
                            if (Number.isNaN(d.getTime())) return '';
                            const hours = String(d.getHours()).padStart(2, '0');
                            const minutes = String(d.getMinutes()).padStart(2, '0');
                            const seconds = String(d.getSeconds()).padStart(2, '0');
                            return `${hours}:${minutes}:${seconds}`;
                        }
                    }
                },
                y: {
                    type: 'linear',
                    suggestedMax: finalYMax !== undefined ? finalYMax + 0.5 : undefined,
                    suggestedMin: finalYMin !== undefined ? finalYMin - 0.5 : undefined
                }
            }
        }
    });
}

async function renderMfcCauseTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    console.log('renderMfcCauseTab - entry:', entry);
    console.log('renderMfcCauseTab - entry.violation_type:', entry?.violation_type, '타입:', typeof entry?.violation_type);
    const vt = Number(entry?.violation_type) || 0;
    console.log('renderMfcCauseTab - 변환된 vt:', vt);
    const headline = buildCauseHeadline(entry);
    const centerRaw = entry.peak_time ?? entry.end ?? entry.start ?? Date.now();
    const center = toMillis(centerRaw) ?? Date.now();
    const windowMs = 15000;
    const startMs = center - windowMs;
    const endMs = center + windowMs;
    const causeTexts = {
        1: 'Baratron 게이지 이상으로 인한 공정 불안정(압력 오차/경보/읽힘 불가)이 확인되었습니다.',
        2: 'MFC Zero Point Drift 발생으로 인해 기준 유량이 정확히 설정되지 않아 실제 유량 편차 및 공정 불안정이 확인되었습니다.',
        3: '보트 엘리베이터 회전부의 Magnetic Seal(자기 유체 씰) 성능 저하로 인해 미세 누설 또는 챔버 내 압력 변동이 발생, 이로 인해 MFC 유량 제어가 불안정해지며 실제 유량 측정값에 편차가 발생한 것으로 판단됩니다.',
        4: 'CKD 밸브 내부의 솔레노이드 코일 또는 밸브 스템(Stem) 구동부에 미세한 이물질 또는 잔류물이 침적되거나, 코일 자체의 열화로 인해 밸브의 정확한 개폐(On/Off) 동작이 지연 또는 불안정해진 것으로 판단됩니다.'
    };
    const label = displayParam(entry?.param || 'MFC');
    const causeText = causeTexts[vt] || '원인 정보가 없습니다.';
    body.innerHTML = `
        <div class="report-cause-layout">
            <div class="report-cause-chart">
                <div class="report-cause-title">${label} 유량 추이 (±5초)</div>
                <canvas id="report-cause-canvas" aria-label="${label} 이상 구간 차트"></canvas>
            </div>
            <div class="report-cause-text">
                <div class="report-cause-badge">원인 진단</div>
                <div class="report-cause-lines">
                    <p>${headline}</p>
                    <p>${causeText}</p>
                </div>
            </div>
        </div>
    `;
    const canvas = document.getElementById('report-cause-canvas');
    await drawReportChart(entry, canvas, { startMs, endMs, windowMs });
}

function renderMfcActionTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    console.log('renderMfcActionTab - entry:', entry);
    console.log('renderMfcActionTab - entry.violation_type:', entry?.violation_type, '타입:', typeof entry?.violation_type);
    const dirMap = {
        1: '1_baratron_gauge',
        2: '2_mfc',
        3: '3_magnetic_seal',
        4: '4_ckd'
    };
    const vt = Number(entry?.violation_type) || 0;
    console.log('renderMfcActionTab - 변환된 vt:', vt);
    const dir = dirMap[vt] || '2_mfc';
    console.log('renderMfcActionTab - 선택된 디렉토리:', dir);
    const label = displayParam(entry?.param || 'MFC');
    body.innerHTML = `
        <div class="report-action-block">
            <div style="font-weight:700; margin-bottom:6px;">조치 방법 (${label})</div>
            <div class="report-markdown" id="mfc-md-container" style="text-align: center; max-width: 100%; margin: 0 auto;">불러오는 중...</div>
        </div>
    `;
    const container = document.getElementById('mfc-md-container');
    const htmlPath = `/static/htmls/${dir}/index.html`;
    loadHtml(htmlPath, `action-${dir}`)
        .then(text => renderHtml(container, text))
        .catch(err => {
            console.error('failed to render html', err);
            if (container) container.textContent = '조치 방법 문서를 불러오지 못했습니다.';
        });
}

// Cover 제어 함수들
function findAllCoverMeshes(scene) {
    if (!scene) return [];
    const coverMeshes = [];
    const coverGroups = [];
    const coverNames = ['대칭_복사cover_1', 'cover', 'Cover', 'COVER'];
    const modelGroup = threeViewer?.modelGroup; // modelGroup 참조 가져오기

    scene.traverse((child) => {
        // modelGroup 자체는 제외
        if (child === modelGroup) return;

        if (child.name) {
            const nameLower = child.name.toLowerCase();
            const isCoverName = coverNames.some(n => nameLower.includes(n.toLowerCase())) || nameLower.includes('cover');

            if (child instanceof THREE.Mesh && isCoverName) {
                coverMeshes.push(child);
                // 부모 그룹도 찾기 (modelGroup 제외)
                let parent = child.parent;
                while (parent && parent !== scene) {
                    // modelGroup은 제외
                    if (parent === modelGroup) break;

                    if (parent instanceof THREE.Group && !coverGroups.includes(parent)) {
                        const parentNameLower = (parent.name || '').toLowerCase();
                        if (parentNameLower.includes('cover') || isCoverName) {
                            // 그룹의 자식 수가 너무 많으면 전체 모델 그룹일 가능성이 있으므로 제외
                            let childCount = 0;
                            parent.traverse(() => childCount++);
                            // 자식이 100개 이상이면 전체 모델 그룹일 가능성이 높으므로 제외
                            if (childCount < 100) {
                                coverGroups.push(parent);
                            }
                        }
                    }
                    parent = parent.parent;
                }
            } else if (child instanceof THREE.Group && isCoverName && !coverGroups.includes(child)) {
                // 그룹의 자식 수가 너무 많으면 전체 모델 그룹일 가능성이 있으므로 제외
                let childCount = 0;
                child.traverse(() => childCount++);
                // 자식이 100개 이상이면 전체 모델 그룹일 가능성이 높으므로 제외
                if (childCount < 100) {
                    coverGroups.push(child);
                }
            }
        }
    });
    // 그룹과 메쉬를 모두 반환
    return { meshes: coverMeshes, groups: coverGroups };
}

function controlCover(action) {
    if (!threeViewer || !threeViewer.scene) {
        console.warn('3D 뷰어가 초기화되지 않았습니다.');
        return;
    }

    const coverData = findAllCoverMeshes(threeViewer.scene);
    const coverMeshes = coverData.meshes || [];
    const coverGroups = coverData.groups || [];

    if (coverMeshes.length === 0 && coverGroups.length === 0) {
        console.warn('Cover 메쉬를 찾을 수 없습니다.');
        // 모든 메쉬 이름 출력 (디버깅)
        const allNames = [];
        threeViewer.scene.traverse((child) => {
            if (child.name) allNames.push(child.name);
        });
        console.log('사용 가능한 메쉬 이름:', allNames);
        return;
    }

    console.log(`Cover 메쉬 ${coverMeshes.length}개, 그룹 ${coverGroups.length}개 발견:`, {
        meshes: coverMeshes.map(m => m.name),
        groups: coverGroups.map(g => g.name)
    });

    // 모든 Cover 메쉬에 대해 동일한 작업 수행
    coverMeshes.forEach((coverMesh) => {
        if (action === 'hide') {
            // 원본 상태 저장
            if (coverMesh.userData.originalVisibility === undefined) {
                coverMesh.userData.originalVisibility = coverMesh.visible;
            }
            coverMesh.visible = false;
        } else if (action === 'show') {
            // 원본 상태로 복원
            if (coverMesh.userData.originalVisibility !== undefined) {
                coverMesh.visible = coverMesh.userData.originalVisibility;
            } else {
                coverMesh.visible = true;
            }
            // 투명도도 원래대로
            if (coverMesh.material) {
                const materials = Array.isArray(coverMesh.material) ? coverMesh.material : [coverMesh.material];
                materials.forEach((mat) => {
                    if (mat instanceof THREE.MeshStandardMaterial || mat instanceof THREE.MeshPhysicalMaterial) {
                        if (mat.userData.originalOpacity !== undefined) {
                            mat.opacity = mat.userData.originalOpacity;
                            mat.transparent = false;
                        }
                    }
                });
            }
        } else if (action === 'transparent') {
            // 투명 처리를 위해서는 Cover를 보이게 해야 함
            // 원본 상태 저장
            if (coverMesh.userData.originalVisibility === undefined) {
                coverMesh.userData.originalVisibility = coverMesh.visible;
            }
            coverMesh.visible = true;

            // 원본 투명도 저장 및 투명 처리
            if (coverMesh.material) {
                const materials = Array.isArray(coverMesh.material) ? coverMesh.material : [coverMesh.material];
                materials.forEach((mat) => {
                    if (mat instanceof THREE.MeshStandardMaterial || mat instanceof THREE.MeshPhysicalMaterial) {
                        if (mat.userData.originalOpacity === undefined) {
                            mat.userData.originalOpacity = mat.opacity;
                        }
                        mat.transparent = true;
                        mat.opacity = 0.3; // 30% 투명도
                    }
                });
            }
        } else if (action === 'reset') {
            // 투명도를 원래대로 복원하되, 현재 visible 상태는 유지
            if (coverMesh.material) {
                const materials = Array.isArray(coverMesh.material) ? coverMesh.material : [coverMesh.material];
                materials.forEach((mat) => {
                    if (mat instanceof THREE.MeshStandardMaterial || mat instanceof THREE.MeshPhysicalMaterial) {
                        if (mat.userData.originalOpacity !== undefined) {
                            mat.opacity = mat.userData.originalOpacity;
                            mat.transparent = false;
                        } else {
                            mat.opacity = 1.0;
                            mat.transparent = false;
                        }
                    }
                });
            }
        }
    });

    // Cover 그룹도 처리
    coverGroups.forEach((coverGroup) => {
        if (action === 'hide') {
            // 원본 상태 저장
            if (coverGroup.userData.originalVisibility === undefined) {
                coverGroup.userData.originalVisibility = coverGroup.visible;
            }
            coverGroup.visible = false;
        } else if (action === 'show') {
            // 원본 상태로 복원
            if (coverGroup.userData.originalVisibility !== undefined) {
                coverGroup.visible = coverGroup.userData.originalVisibility;
            } else {
                coverGroup.visible = true;
            }
        } else if (action === 'transparent') {
            // 투명 처리를 위해서는 Cover 그룹을 보이게 해야 함
            if (coverGroup.userData.originalVisibility === undefined) {
                coverGroup.userData.originalVisibility = coverGroup.visible;
            }
            coverGroup.visible = true;
        }
        // reset 액션은 그룹에 대해서는 메쉬만 처리하면 됨
    });

    console.log(`Cover ${action} 처리 완료 (메쉬 ${coverMeshes.length}개, 그룹 ${coverGroups.length}개)`);
}

function renderMfcDrawingTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    const label = displayParam(entry?.param || 'MFC');
    // ALD Batch Type Assy9.gltf 파일 사용 (.bin 파일은 자동으로 참조됨)
    const encoded = encodeURIComponent('ALD Batch Type Assy9.gltf');
    const modelUrl = `/static/3D/${encoded}`;
    body.innerHTML = `
        <div class="report-3d-wrap">
            <div class="report-3d-header">
                <span>도면 (3D)</span>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <button id="cover-hide-btn" style="padding: 6px 12px; background: #3b82f6; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; min-width: 110px; width: 110px; text-align: center;">Cover 숨기기</button>
                    <button id="cover-show-btn" style="padding: 6px 12px; background: #3b82f6; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; display: none; min-width: 110px; width: 110px; text-align: center;">Cover 보이기</button>
                    <button id="cover-transparent-btn" style="padding: 6px 12px; background: #9333ea; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; min-width: 110px; width: 110px; text-align: center;">Cover 투명하게</button>
                </div>
            </div>
            <div class="report-3d-canvas" id="report-3d-container">로딩 중...</div>
            <div class="report-3d-note">마우스 드래그: 회전 · 스크롤: 줌 · 오른쪽 버튼: 이동 (대용량 파일은 로딩이 다소 지연될 수 있습니다)</div>
        </div>
    `;
    const container = document.getElementById('report-3d-container');

    // 버튼 이벤트 리스너 설정
    const hideBtn = document.getElementById('cover-hide-btn');
    const showBtn = document.getElementById('cover-show-btn');
    const transparentBtn = document.getElementById('cover-transparent-btn');

    let coverVisible = true; // Cover가 보이는지 여부
    let coverTransparent = false; // Cover가 투명한지 여부

    const updateButtonStates = () => {
        // 숨기기/보이기 버튼 토글
        if (coverVisible) {
            hideBtn.style.display = 'inline-block';
            showBtn.style.display = 'none';
        } else {
            hideBtn.style.display = 'none';
            showBtn.style.display = 'inline-block';
        }

        // 투명 버튼 텍스트 업데이트
        if (coverTransparent) {
            transparentBtn.textContent = '원래대로';
        } else {
            transparentBtn.textContent = 'Cover 투명하게';
        }
    };

    hideBtn?.addEventListener('click', () => {
        controlCover('hide');
        coverVisible = false;
        updateButtonStates();
    });

    showBtn?.addEventListener('click', () => {
        controlCover('show');
        coverVisible = true;
        updateButtonStates();
    });

    transparentBtn?.addEventListener('click', () => {
        if (coverTransparent) {
            // 투명 상태에서 원래대로 클릭 → 불투명 처리
            controlCover('reset');
            coverTransparent = false;
            // 불투명 처리 후 원래 visible 상태로 복원
            if (threeViewer && threeViewer.scene) {
                const coverData = findAllCoverMeshes(threeViewer.scene);
                const coverMeshes = coverData.meshes || [];
                if (coverMeshes.length > 0 && coverMeshes[0].userData.originalVisibility !== undefined) {
                    // 첫 번째 메쉬의 원래 상태를 기준으로 설정
                    coverVisible = coverMeshes[0].userData.originalVisibility;
                    // 모든 메쉬를 원래 상태로 복원
                    coverMeshes.forEach(mesh => {
                        if (mesh.userData.originalVisibility !== undefined) {
                            mesh.visible = mesh.userData.originalVisibility;
                        }
                    });
                }
            }
        } else {
            // 투명하게 클릭 → 투명 처리 (Cover를 보이게 함)
            controlCover('transparent');
            coverTransparent = true;
            coverVisible = true; // 투명 처리 시 항상 보이게
        }
        updateButtonStates();
    });

    // 3D 뷰어 생성
    createThreeViewer(container, modelUrl, entry);

    // 모델 로드 완료 후 Cover 메쉬 확인 (약간의 지연 후)
    setTimeout(() => {
        if (threeViewer && threeViewer.scene) {
            const coverData = findAllCoverMeshes(threeViewer.scene);
            const coverMeshes = coverData.meshes || [];
            const coverGroups = coverData.groups || [];
            if (coverMeshes.length > 0 || coverGroups.length > 0) {
                console.log(`Cover 메쉬 ${coverMeshes.length}개, 그룹 ${coverGroups.length}개 찾음:`, {
                    meshes: coverMeshes.map(m => m.name),
                    groups: coverGroups.map(g => g.name)
                });
            } else {
                console.warn('Cover 메쉬를 찾을 수 없습니다.');
            }
        }
    }, 2000);
}

function getPartsData(entry) {
    console.log('getPartsData - entry:', entry);
    console.log('getPartsData - entry.violation_type:', entry?.violation_type, '타입:', typeof entry?.violation_type);
    const vt = Number(entry?.violation_type) || 0;
    console.log('getPartsData - 변환된 vt:', vt);
    if (partsCatalog.byViolation[vt]) {
        console.log('getPartsData - 선택된 부품 정보:', partsCatalog.byViolation[vt].title);
        const data = partsCatalog.byViolation[vt];

        // violation_type이 1, 3, 4일 때는 수정 전 목록(oldVendors) 사용
        if ((vt === 1 || vt === 3 || vt === 4) && data.oldVendors) {
            console.log('getPartsData - 수정 전 목록 사용 (violation_type:', vt, ')');
            // oldVendors를 사용하는 새로운 객체 반환
            return {
                ...data,
                vendors: data.oldVendors
            };
        }

        return data;
    }
    console.log('getPartsData - 기본 부품 정보 사용');
    return partsCatalog.default;
}

function renderPartsTab(entry) {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    const data = getPartsData(entry);

    // 판매처 데이터 준비 (NO 칼럼 제거)
    const vendors = data.vendors || [];
    const itemsPerPage = 4;
    const totalPages = Math.ceil(vendors.length / itemsPerPage);

    // 현재 페이지의 데이터만 렌더링하는 함수
    const renderVendorTable = (page = 1) => {
        const startIdx = (page - 1) * itemsPerPage;
        const endIdx = startIdx + itemsPerPage;
        const pageVendors = vendors.slice(startIdx, endIdx);

        // 제품 문의처에서 TEL과 E-MAIL 개행 처리
        const formatContact = (contact) => {
            if (!contact) return '';
            // E MAIL, E-MAIL, EMAIL 패턴을 <br>로 변경
            return contact.replace(/\s+(E\s*[-\s]?MAIL|EMAIL)\s*:\s*/gi, '<br>$1: ');
        };

        const vendorRows = pageVendors.map((v, idx) => {
            const no = startIdx + idx + 1;
            const formattedContact = formatContact(v.contact);
            return `
            <tr>
                <td>${no}</td>
                <td>${v.name ?? ''}</td>
                <td>${v.biz ?? ''}</td>
                <td>${v.link ? `<a href="${v.link}" target="_blank" rel="noopener">${v.link}</a>` : ''}</td>
                <td>${formattedContact}</td>
            </tr>
        `;
        }).join('');

        // 페이지네이션 버튼 생성
        let paginationHtml = '';
        if (totalPages > 1) {
            const pageButtons = [];
            for (let i = 1; i <= totalPages; i++) {
                pageButtons.push(`<button class="vendor-page-btn ${i === page ? 'active' : ''}" data-page="${i}">${i}</button>`);
            }
            paginationHtml = `<div class="vendor-pagination">${pageButtons.join('')}</div>`;
        }

        return { vendorRows, paginationHtml };
    };

    // 초기 렌더링 (1페이지)
    const { vendorRows, paginationHtml } = renderVendorTable(1);

    const imageHtml = data.image ? `
        <figure class="parts-figure">
            <img src="${data.image.src}" alt="${data.image.alt || data.title || ''}">
            <figcaption>${data.image.caption || ''}</figcaption>
        </figure>
    ` : '';

    // description을 글머리 기호 리스트로 변환
    let descriptionHtml = '';
    if (data.description) {
        const lines = data.description.split('\n').filter(line => line.trim());
        const listItems = lines.map(line => {
            // '- '로 시작하는 경우 제거하고 내용만 사용
            const content = line.replace(/^-\s*/, '').trim();
            return `<li>${content}</li>`;
        }).join('');
        descriptionHtml = `<div class="parts-text-block"><ul class="parts-description-list">${listItems}</ul></div>`;
    }

    // 판매 정보 탭 HTML 생성
    let salesInfoHtml = '<div class="parts-note">판매 정보가 없습니다.</div>';
    if (data.sales && data.sales.iframeUrl) {
        const salesText = data.sales.text || '';
        const imageUrl = data.sales.imageUrl || '';

        // 판매 정보 텍스트를 글머리 기호 리스트로 변환
        let salesTextHtml = '';
        if (salesText) {
            const lines = salesText.split('\n').filter(line => line.trim());
            let titleHtml = '';
            const listItems = [];
            let foundTitle = false;

            lines.forEach(line => {
                const trimmed = line.trim();
                // '[이상 로그 연계 판단]' 같은 제목 찾기
                if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
                    titleHtml = `<div class="sales-info-title">${trimmed}</div>`;
                    foundTitle = true;
                } else if (foundTitle && trimmed) {
                    // 제목 이후의 모든 줄을 리스트 항목으로 추가
                    const content = trimmed.replace(/^-\s*/, '').trim();
                    if (content) {
                        listItems.push(`<li>${content}</li>`);
                    }
                }
            });

            // 리스트가 있으면 ul로 감싸기
            if (listItems.length > 0) {
                salesTextHtml = `${titleHtml}<ul class="sales-info-list">${listItems.join('')}</ul>`;
            } else {
                salesTextHtml = titleHtml || salesText.replace(/\n/g, '<br>');
            }
        }

        salesInfoHtml = `
            <div class="sales-layout">
                <div class="sales-image-container">
                    ${imageUrl ? `<img src="${imageUrl}" alt="상품 이미지" class="sales-image" onclick="window.open('${data.sales.iframeUrl}', '_blank')" style="cursor: pointer;">` : ''}
                </div>
            </div>
        `;
    }
    body.innerHTML = `
        <div class="parts-tablist" role="tablist" aria-label="부품 확인 탭">
            <button class="parts-subtab active" data-sub="info" role="tab" aria-selected="true">부품 정보</button>
            <button class="parts-subtab" data-sub="sales" role="tab" aria-selected="false">판매 정보</button>
            <button class="parts-subtab" data-sub="vendor" role="tab" aria-selected="false">판매처 리스트</button>
        </div>
        <div class="parts-panel active" data-sub="info" role="tabpanel">
            <div class="parts-section">
                <div class="parts-info-layout process-layout">
                    ${imageHtml}
                    <div class="parts-text-cols">
                        <div class="parts-text-content">
                            ${descriptionHtml || '<div class="parts-note">부품 정보가 없습니다.</div>'}
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div class="parts-panel" data-sub="sales" role="tabpanel">
            ${salesInfoHtml}
        </div>
        <div class="parts-panel" data-sub="vendor" role="tabpanel">
            ${vendors.length > 0 ? `
                <div class="parts-table-wrapper">
                    <div class="parts-table-container">
                        <div class="parts-table-inner">
                            <table class="parts-table">
                                <thead>
                                    <tr><th>No.</th><th>업체명</th><th>사업자번호</th><th>제품상세정보</th><th>제품문의처</th></tr>
                                </thead>
                                <tbody id="vendor-table-body">
                                    ${vendorRows}
                                </tbody>
                            </table>
                        </div>
                        ${paginationHtml}
                    </div>
                </div>
            ` : '<div class="parts-note">등록된 판매처 리스트 정보가 없습니다.</div>'}
        </div>
    `;

    // 페이지네이션 버튼 이벤트 처리
    if (vendors.length > 0 && totalPages > 1) {
        const pageButtons = body.querySelectorAll('.vendor-page-btn');
        const vendorTableBody = body.querySelector('#vendor-table-body');

        pageButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                const page = parseInt(btn.dataset.page);
                const { vendorRows: newRows } = renderVendorTable(page);

                // 테이블 내용 업데이트
                if (vendorTableBody) {
                    vendorTableBody.innerHTML = newRows;
                }

                // 버튼 활성화 상태 업데이트
                pageButtons.forEach(b => {
                    b.classList.toggle('active', parseInt(b.dataset.page) === page);
                });

                // 테이블 높이 조정
                setTimeout(() => adjustPartsTableHeight(), 100);
            });
        });
    }

    const tabs = body.querySelectorAll('.parts-subtab');
    const panels = body.querySelectorAll('.parts-panel');
    tabs.forEach(btn => {
        btn.addEventListener('click', () => {
            const target = btn.dataset.sub;
            tabs.forEach(b => {
                const active = b === btn;
                b.classList.toggle('active', active);
                b.setAttribute('aria-selected', active ? 'true' : 'false');
            });
            panels.forEach(panel => {
                panel.classList.toggle('active', panel.dataset.sub === target);
            });
            // 판매처 리스트 탭이 활성화되면 테이블 높이 조정
            if (target === 'vendor') {
                setTimeout(() => adjustPartsTableHeight(), 100);
            }
        });
    });

    // 판매처 리스트 탭이 처음 렌더링될 때도 높이 조정
    const vendorPanel = body.querySelector('.parts-panel[data-sub="vendor"]');
    if (vendorPanel && vendorPanel.classList.contains('active')) {
        setTimeout(() => adjustPartsTableHeight(), 100);
    }

    // 부품정보 탭의 글씨 크기 동적 조정 및 가운데 정렬
    const infoPanel = body.querySelector('.parts-panel[data-sub="info"]');
    if (infoPanel && infoPanel.classList.contains('active')) {
        setTimeout(() => {
            adjustPartsInfoTextSize();
            // 부품정보 탭에서 parts-section-title이 없으면 가운데 정렬
            const textCols = infoPanel.querySelector('.parts-text-cols');
            if (textCols && !textCols.querySelector('.parts-section-title')) {
                textCols.classList.add('parts-info-center');
            }
        }, 100);
    }
}

function adjustPartsTableHeight() {
    const tableWrapper = document.querySelector('.parts-table-wrapper');
    const table = document.querySelector('.parts-table');
    const tbody = table?.querySelector('tbody');
    if (!tableWrapper || !table || !tbody) return;

    // wrapper의 실제 사용 가능한 높이 계산
    const wrapperHeight = tableWrapper.clientHeight;

    const thead = table.querySelector('thead');
    const theadHeight = thead?.offsetHeight || 0;
    const availableHeight = wrapperHeight - theadHeight;
    const rowCount = tbody.querySelectorAll('tr').length;

    if (rowCount > 0 && availableHeight > 0) {
        // 테이블 높이를 wrapper에 정확히 맞춤
        table.style.height = wrapperHeight + 'px';
        table.style.maxHeight = wrapperHeight + 'px';

        // 각 행의 높이를 균등하게 분배 (여유 공간 고려)
        const rowHeight = Math.max(40, Math.floor(availableHeight / rowCount));
        tbody.querySelectorAll('tr').forEach(tr => {
            tr.style.height = rowHeight + 'px';
        });

        // 테이블 너비가 컨테이너를 넘지 않도록
        const containerWidth = tableWrapper.clientWidth;
        table.style.width = '100%';
        table.style.maxWidth = containerWidth + 'px';
        table.style.boxSizing = 'border-box';

        // thead와 tbody의 칼럼 너비를 정확히 맞추기
        if (thead) {
            const theadCells = thead.querySelectorAll('th');
            const firstRowCells = tbody.querySelector('tr')?.querySelectorAll('td');
            if (theadCells.length === firstRowCells?.length && firstRowCells.length > 0) {
                // 테이블의 실제 사용 가능한 너비 계산 (border 제외)
                const tableStyle = window.getComputedStyle(table);
                const tableBorder = parseFloat(tableStyle.borderLeftWidth || 0) + parseFloat(tableStyle.borderRightWidth || 0);
                const availableWidth = containerWidth - tableBorder;
                const columnCount = theadCells.length;
                const columnWidth = Math.floor(availableWidth / columnCount);

                // 모든 thead와 tbody 셀의 너비를 동일하게 설정
                theadCells.forEach((th, idx) => {
                    th.style.width = columnWidth + 'px';
                    th.style.minWidth = columnWidth + 'px';
                    th.style.maxWidth = columnWidth + 'px';
                    th.style.boxSizing = 'border-box';
                });

                tbody.querySelectorAll('tr').forEach(tr => {
                    const cells = tr.querySelectorAll('td');
                    cells.forEach((td, idx) => {
                        if (idx < columnCount) {
                            td.style.width = columnWidth + 'px';
                            td.style.minWidth = columnWidth + 'px';
                            td.style.maxWidth = columnWidth + 'px';
                            td.style.boxSizing = 'border-box';
                        }
                    });
                });
            }
        }
    }
}

function renderProcessTab() {
    const body = reportElements.reportBody;
    if (!body) return;
    destroyReportChart();
    disposeThreeViewer();
    if (!Array.isArray(processSteps) || processSteps.length === 0) {
        body.textContent = '공정 단계 설명이 없습니다.';
        return;
    }
    const step = processSteps[Math.floor(Math.random() * processSteps.length)];
    const sections = (step.sections || []).map(sec => `
        <div class="parts-text-block">
            <div class="label">${sec.label}</div>
            <div>${sec.text}</div>
        </div>
    `).join('');
    body.innerHTML = `
        <div class="parts-section">
            <div class="parts-info-layout process-layout">
                <figure class="parts-figure">
                    <img src="${step.image?.src || ''}" alt="${step.image?.alt || ''}">
                    <figcaption>${step.image?.caption || ''}</figcaption>
                </figure>
                <div class="parts-text-cols">
                    <div class="parts-section-title">${step.title}${step.subtitle ? ` (${step.subtitle})` : ''}</div>
                    <div class="parts-text-content">
                        ${sections || '<div class="parts-note">설명 정보가 없습니다.</div>'}
                    </div>
                </div>
            </div>
        </div>
    `;

    // 화면 크기에 따라 글씨 크기 동적 조정
    adjustProcessTextSize();
}

function adjustProcessTextSize() {
    const textContent = document.querySelector('.process-layout .parts-text-content');
    if (!textContent) return;

    const textBlocks = textContent.querySelectorAll('.parts-text-block');
    if (textBlocks.length === 0) return;

    // 초기 글씨 크기
    let fontSize = 14;
    let labelFontSize = 15;
    const minFontSize = 9;
    const minLabelFontSize = 10;

    // 컨테이너 높이 확인
    const container = textContent.closest('.parts-text-cols');
    if (!container) return;

    const checkFit = () => {
        // 현재 글씨 크기 적용
        textBlocks.forEach(block => {
            block.style.fontSize = fontSize + 'px';
            const label = block.querySelector('.label');
            if (label) {
                label.style.fontSize = labelFontSize + 'px';
            }
        });

        // 내용이 넘치는지 확인
        const isOverflowing = textContent.scrollHeight > textContent.clientHeight;

        if (isOverflowing && fontSize > minFontSize) {
            // 글씨 크기 줄이기
            fontSize = Math.max(minFontSize, fontSize - 1);
            labelFontSize = Math.max(minLabelFontSize, labelFontSize - 1);

            // 재귀적으로 다시 확인
            setTimeout(checkFit, 0);
        }
    };

    // 이미지 로드 후 확인
    const img = document.querySelector('.process-layout .parts-figure img');
    if (img && !img.complete) {
        img.addEventListener('load', () => {
            setTimeout(checkFit, 100);
        });
    } else {
        setTimeout(checkFit, 100);
    }

    // 창 크기 변경 시 재조정
    let resizeTimeout;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            fontSize = 14;
            labelFontSize = 15;
            checkFit();
        }, 200);
    });
}

function adjustPartsInfoTextSize() {
    const textContent = document.querySelector('.parts-panel[data-sub="info"] .parts-text-content');
    if (!textContent) return;

    const textBlocks = textContent.querySelectorAll('.parts-text-block');
    const descriptionList = textContent.querySelector('.parts-description-list');
    if (textBlocks.length === 0 && !descriptionList) return;

    // 초기 글씨 크기
    let fontSize = 14;
    const minFontSize = 9;

    const checkFit = () => {
        // 현재 글씨 크기 적용
        textBlocks.forEach(block => {
            block.style.fontSize = fontSize + 'px';
        });

        if (descriptionList) {
            descriptionList.style.fontSize = fontSize + 'px';
        }

        // 내용이 넘치는지 확인
        const isOverflowing = textContent.scrollHeight > textContent.clientHeight;

        if (isOverflowing && fontSize > minFontSize) {
            // 글씨 크기 줄이기
            fontSize = Math.max(minFontSize, fontSize - 1);

            // 재귀적으로 다시 확인
            setTimeout(checkFit, 0);
        }
    };

    // 이미지 로드 후 확인
    const img = document.querySelector('.parts-panel[data-sub="info"] .parts-figure img');
    if (img && !img.complete) {
        img.addEventListener('load', () => {
            setTimeout(checkFit, 100);
        });
    } else {
        setTimeout(checkFit, 100);
    }

    // 창 크기 변경 시 재조정
    let resizeTimeout;
    const resizeHandler = () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            fontSize = 14;
            checkFit();
        }, 200);
    };

    window.addEventListener('resize', resizeHandler);

    // 탭 전환 시에도 재조정
    const tabs = document.querySelectorAll('.parts-subtab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            if (tab.dataset.sub === 'info') {
                setTimeout(() => {
                    fontSize = 14;
                    checkFit();
                }, 100);
            }
        });
    });
}

async function setActiveReportTab(tabKey) {
    const tabs = document.querySelectorAll('.report-tab');
    tabs.forEach(t => {
        if (t.dataset.tab === tabKey) t.classList.add('active');
        else t.classList.remove('active');
    });
    const body = reportElements.reportBody;
    if (!body || !currentReportEntry) return;
    const base = `${displayParam(currentReportEntry.param)} 관련 리포트가 준비되지 않았습니다.`;
    const defaultMap = {
        parts: '부품 확인 정보가 없습니다.',
        drawing: '도면(이상 위치) 정보가 없습니다.',
        process: '공정 단계 설명이 없습니다.'
    };
    try {
        if (tabKey === 'cause') {
            await renderMfcCauseTab(currentReportEntry);
        } else if (tabKey === 'action') {
            renderMfcActionTab(currentReportEntry);
        } else if (tabKey === 'parts') {
            renderPartsTab(currentReportEntry);
            setTimeout(() => adjustPartsTableHeight(), 200);
        } else if (tabKey === 'drawing') {
            if (mfcParams.has(currentReportEntry.param)) {
                renderMfcDrawingTab(currentReportEntry);
            } else {
                destroyReportChart();
                disposeThreeViewer();
                body.textContent = defaultMap[tabKey] || base;
            }
        } else if (tabKey === 'process') {
            renderProcessTab();
        } else {
            destroyReportChart();
            disposeThreeViewer();
            body.textContent = defaultMap[tabKey] || base;
        }
    } catch (err) {
        console.error('failed to render report tab', err);
        body.textContent = '리포트 내용을 불러오지 못했습니다.';
    }
}

async function openReportModal(entry) {
    if (!reportElements.reportModal || !reportElements.reportSummary || !reportElements.reportBody) return;

    // DB에서 최신 violation_type 가져오기
    try {
        // entry의 start와 end를 사용하여 해당 기간의 이상 로그 조회
        // 시간 범위를 넓히기 위해 ±5초 여유를 둠
        const searchStart = entry.start ? entry.start - 5000 : null;
        const searchEnd = entry.end ? entry.end + 5000 : null;

        if (searchStart && searchEnd) {
            // 로컬 시간 형식으로 변환 (YYYY-MM-DD HH:MM:SS)
            const formatLocalTime = (timestamp) => {
                const d = new Date(timestamp);
                const year = d.getFullYear();
                const month = String(d.getMonth() + 1).padStart(2, '0');
                const day = String(d.getDate()).padStart(2, '0');
                const hours = String(d.getHours()).padStart(2, '0');
                const minutes = String(d.getMinutes()).padStart(2, '0');
                const seconds = String(d.getSeconds()).padStart(2, '0');
                return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
            };

            const startTime = formatLocalTime(searchStart);
            const endTime = formatLocalTime(searchEnd);

            console.log('리포트 모달 - 조회 시간 범위:', { startTime, endTime });
            console.log('리포트 모달 - entry 시간:', {
                entryStart: formatLocalTime(entry.start),
                entryEnd: formatLocalTime(entry.end)
            });

            const timestamp = new Date().getTime(); // 캐시 방지용 타임스탬프
            const res = await fetch(`/api/alarm_history?process_start_time=${encodeURIComponent(startTime)}&process_end_time=${encodeURIComponent(endTime)}&_t=${timestamp}`, {
                cache: 'no-cache',
                headers: {
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }
            });

            if (res.ok) {
                const alarmData = await res.json();
                console.log('리포트 모달 - DB에서 가져온 이상 로그:', alarmData);
                console.log('리포트 모달 - 조회된 이상 로그 개수:', alarmData ? alarmData.length : 0);

                // 해당 파라미터와 시간이 일치하는 이상 로그 찾기
                if (alarmData && alarmData.length > 0) {
                    console.log('리포트 모달 - entry.param:', entry.param);

                    // 파라미터가 일치하는 이상 로그 필터링
                    const paramMatches = alarmData.filter(alarm => alarm.parameter === entry.param);
                    console.log('리포트 모달 - 파라미터 일치하는 이상 로그:', paramMatches);

                    if (paramMatches.length > 0) {
                        // 가장 최근의 이상 로그 사용 (end_time 기준)
                        const sorted = [...paramMatches].sort((a, b) => {
                            const timeA = new Date(a.end_time || a.start_time || 0);
                            const timeB = new Date(b.end_time || b.start_time || 0);
                            return timeB - timeA;
                        });

                        // 시간 범위가 겹치는 이상 로그 찾기
                        let matchedAlarm = sorted.find(alarm => {
                            // 시간 문자열을 Date 객체로 변환
                            const parseTime = (timeStr) => {
                                if (!timeStr) return null;
                                // "YYYY-MM-DD HH:MM:SS" 형식 파싱
                                const [datePart, timePart] = timeStr.split(' ');
                                if (!datePart || !timePart) return null;
                                const [year, month, day] = datePart.split('-').map(Number);
                                const [hours, minutes, seconds] = timePart.split(':').map(Number);
                                return new Date(year, month - 1, day, hours, minutes, seconds);
                            };

                            const alarmStart = parseTime(alarm.start_time);
                            const alarmEnd = parseTime(alarm.end_time);
                            const entryStart = new Date(entry.start);
                            const entryEnd = new Date(entry.end);

                            if (!alarmStart || !alarmEnd) return false;

                            // 시간 범위가 겹치는지 확인 (5초 여유)
                            const overlap = alarmStart <= entryEnd + 5000 && alarmEnd >= entryStart - 5000;
                            console.log('리포트 모달 - 시간 매칭 확인:', {
                                alarm: { start: alarm.start_time, end: alarm.end_time },
                                entry: { start: formatLocalTime(entry.start), end: formatLocalTime(entry.end) },
                                overlap
                            });
                            return overlap;
                        });

                        // 시간 매칭이 없으면 가장 최근 것 사용
                        if (!matchedAlarm && sorted.length > 0) {
                            matchedAlarm = sorted[0];
                            console.log('리포트 모달 - 시간 매칭 없음, 가장 최근 것 사용:', matchedAlarm);
                        }

                        if (matchedAlarm && matchedAlarm.violation_type !== null && matchedAlarm.violation_type !== undefined) {
                            // DB에서 가져온 최신 violation_type으로 업데이트
                            const oldVt = entry.violation_type;
                            entry.violation_type = matchedAlarm.violation_type;
                            console.log('리포트 모달 - violation_type 업데이트:', {
                                old: oldVt,
                                new: matchedAlarm.violation_type,
                                alarm: matchedAlarm
                            });
                        } else {
                            console.log('리포트 모달 - violation_type 업데이트 실패: matchedAlarm 없음');
                        }
                    } else {
                        console.log('리포트 모달 - 파라미터 일치하는 이상 로그 없음');
                    }
                } else {
                    console.log('리포트 모달 - 조회된 이상 로그 없음');
                }
            }
        }
    } catch (e) {
        console.error('리포트 모달 - DB에서 violation_type 가져오기 실패:', e);
        // 실패해도 기존 entry 사용
    }

    currentReportEntry = entry;
    const timeText = formatTimelineTime(entry.end);
    const diff = entry.diff != null ? Math.abs(entry.diff).toFixed(0) : '0';
    const severity = buildSeverity(Number(diff));
    const logText = buildLogText(entry.param, entry);
    const summaryLines = [
        '이상 요약 (Summary)',
        `- 이상 감지 시간 : ${timeText}`,
        `- 이상 유형 : ${logText}`
    ];
    reportElements.reportSummary.textContent = summaryLines.join('\n');
    setActiveReportTab('cause');
    reportElements.reportModal.style.display = 'flex';
}

function closeReportModal() {
    if (reportElements.reportModal) reportElements.reportModal.style.display = 'none';
    destroyReportChart();
    disposeThreeViewer();
    currentReportEntry = null;
}

async function fetchAbnormalLogs() {
    try {
        const res = await fetch('/api/anomaly_logs');
        if (!res.ok) {
            const errorText = await res.text();
            console.error('failed to fetch abnormal logs - bad response:', res.status, res.statusText, errorText);
            throw new Error(`bad response: ${res.status} ${res.statusText}`);
        }
        const data = await res.json();
        console.log('fetchAbnormalLogs - received data:', data?.length || 0, 'items');
        setServerLogs(Array.isArray(data) ? data : []);
    } catch (e) {
        console.error('failed to fetch abnormal logs', e);
    }
}

async function fetchData() {
    if (!processStart) return;
    if (chartHoldEnabled) return; // HOLD가 켜져있으면 업데이트 중지
    const now = new Date();
    const processStartTime = processStart ? new Date(processStart).getTime() : 0;
    const timeRangeMs = selectedTimeRange * 60 * 1000; // 선택된 시간 범위를 밀리초로 변환
    const windowStartMs = Math.max(processStartTime, now.getTime() - timeRangeMs);
    const windowEndMs = now.getTime();
    const startIso = new Date(windowStartMs).toISOString();
    const nowIso = new Date(windowEndMs).toISOString();
    updateTimeRangeLabel(windowStartMs, windowEndMs);

    // limits가 비어있으면 로드
    if (Object.keys(limits).length === 0) {
        try {
            const res = await fetch('/api/limits');
            if (res.ok) {
                const json = await res.json();
                limits = json.limits || json || {};
            }
        } catch (e) {
            console.error('failed to load limits', e);
        }
    }

    // interlockLimits가 비어있으면 로드
    if (Object.keys(interlockLimits).length === 0) {
        try {
            const res = await fetch('/api/interlock_limits');
            if (res.ok) {
                const json = await res.json();
                interlockLimits = json.limits || json || {};
            }
        } catch (e) {
            console.error('failed to load interlock limits', e);
        }
    }

    // 모든 차트 데이터를 가져온 후 업데이트
    const chartPromises = columns.map(async col => {
        try {
            const res = await fetch(`/api/event_chart?param=${encodeURIComponent(col)}&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(nowIso)}`);
            const json = await res.json();
            updateCharts(col, json);
        } catch (e) {
            console.error(`failed to fetch chart data for ${col}`, e);
        }
    });
    await Promise.all(chartPromises);
}

function checkProcess() {
    fetch(`/api/process_range?time=${encodeURIComponent(new Date().toISOString())}`)
        .then(res => res.json())
        .then(r => {
            if (!processStart) {
                processStart = r.start;
                processEnd = r.end;
            } else if (processStart !== r.start) {
                processStart = r.start;
                processEnd = r.end;
                logs = {}; loggedIds.clear();
                updateLog();
                Object.values(mainCharts).forEach(c => { c.data.datasets.forEach(ds => ds.data = []); c.options.plugins.highlightRegion.regions = []; c.update(); });
            } else {
                processEnd = r.end;
            }
        });
}

window.addEventListener('DOMContentLoaded', () => {
    applyDisplayLabels();
    loadSettings().finally(() => {
        const warningToggle = document.getElementById('warning-toggle');
        const stored = localStorage.getItem(warningToggleKey);
        if (settingsCache && typeof settingsCache.warning_enabled === 'boolean') {
            warningEnabled = settingsCache.warning_enabled;
        } else if (stored !== null) {
            warningEnabled = stored === 'true';
        }
        if (warningToggle) warningToggle.checked = warningEnabled;
        localStorage.setItem(warningToggleKey, String(warningEnabled));
    });
    createCharts();
    updateLogPanelHeight();
    checkProcess();
    //setInterval(checkProcess, 20000);
    // limits 로드 후 첫 데이터 가져오기
    loadLimits().then(() => {
        console.log('[상한선/하한선] limits 로드 완료 후 fetchData 호출. limits 키:', Object.keys(limits));
        fetchData();
        setInterval(fetchData, 1000);
    });
    fetchAbnormalLogs();
    setInterval(fetchAbnormalLogs, 2000);
    pollGeneratorStatus();
    setInterval(pollGeneratorStatus, 3000);
    fetchCurrentStepFallback(true);
    setInterval(() => fetchCurrentStepFallback(false), 5000);
    // 상/하한값 설정 기능
    async function loadLimits() {
        try {
            const res = await fetch('/api/limits');
            if (!res.ok) return;
            const json = await res.json();
            limits = json.limits || json || {};
            console.log('[상한선/하한선] loadLimits 완료. 파라미터 수:', Object.keys(limits).length);
            if (Object.keys(limits).length > 0) {
                const firstKey = Object.keys(limits)[0];
                console.log('[상한선/하한선] 첫 번째 파라미터:', firstKey, 'all:', limits[firstKey]?.all);
            }
        } catch (e) {
            console.error('failed to load limits', e);
        }
    }

    function createSettingsUI() {
        const catWrap = document.getElementById("category-buttons");
        const paramWrap = document.getElementById("param-buttons");
        const form = document.getElementById("settings-form");
        if (!catWrap || !paramWrap || !form) return;
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
                btn.textContent = displayParam(col);
                btn.onclick = () => {
                    activeParam = col;
                    renderParams();
                    renderStepTable(col);
                };
                paramWrap.appendChild(btn);
            });
        }

        function renderStepTable(col) {
            form.innerHTML = '';
            const stepIds = Object.keys(stepNames).map(Number).sort((a, b) => a - b);
            const table = document.createElement("table");
            table.className = "step-table";
            const thead = document.createElement("thead");
            thead.innerHTML = `<tr><th>Step ID</th><th>Step Name</th><th>Min</th><th>Max</th></tr>`;
            table.appendChild(thead);
            const tbody = document.createElement("tbody");
            const commonLim = limits?.[col]?.["all"] || {};
            const commonRow = document.createElement("tr");
            commonRow.innerHTML = `
                <td><strong>All</strong></td>
                <td><em>모든 Step 공통</em></td>
                <td><input data-col="${col}" data-step="all" data-type="min" value="${commonLim.min ?? ''}" /></td>
                <td><input data-col="${col}" data-step="all" data-type="max" value="${commonLim.max ?? ''}" /></td>
            `;
            tbody.appendChild(commonRow);
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

        renderCategories();
        renderParams();
        renderStepTable(activeParam);
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
        const updatedPart = collectLimits();
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
            // 모달을 닫지 않고 유지
            fetchData();
        } else {
            alert("저장 실패");
        }
    }

    document.getElementById("open-settings")?.addEventListener("click", () => {
        createSettingsUI();
        document.getElementById("settings-modal").style.display = "block";
    });

    document.getElementById("save-settings")?.addEventListener("click", saveLimits);

    // Interlock 설정 UI 함수
    function createInterlockSettingsUI() {
        const catWrap = document.getElementById("interlock-category-buttons");
        const paramWrap = document.getElementById("interlock-param-buttons");
        const form = document.getElementById("interlock-settings-form");
        if (!catWrap || !paramWrap || !form) return;
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
                    renderInterlockTable(activeParam);
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
                btn.textContent = displayParam(col);
                btn.onclick = () => {
                    activeParam = col;
                    renderParams();
                    renderInterlockTable(col);
                };
                paramWrap.appendChild(btn);
            });
        }

        function renderInterlockTable(col) {
            form.innerHTML = '';
            const table = document.createElement("table");
            table.className = "step-table";
            const thead = document.createElement("thead");
            thead.innerHTML = `<tr><th>Parameter</th><th>Min</th><th>Max</th></tr>`;
            table.appendChild(thead);
            const tbody = document.createElement("tbody");
            const interlockLim = interlockLimits?.[col]?.["all"] || {};
            const row = document.createElement("tr");
            row.innerHTML = `
                <td><strong>${displayParam(col)}</strong></td>
                <td><input data-col="${col}" data-step="all" data-type="min" value="${interlockLim.min ?? ''}" /></td>
                <td><input data-col="${col}" data-step="all" data-type="max" value="${interlockLim.max ?? ''}" /></td>
            `;
            tbody.appendChild(row);
            table.appendChild(tbody);
            form.appendChild(table);
        }

        renderCategories();
        renderParams();
        renderInterlockTable(activeParam);
    }

    function collectInterlockLimits() {
        const inputs = document.querySelectorAll("#interlock-settings-form input");
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

    async function saveInterlockLimits() {
        const updatedPart = collectInterlockLimits();
        const merged = { ...interlockLimits };
        Object.entries(updatedPart).forEach(([col, steps]) => {
            if (!merged[col]) merged[col] = {};
            Object.entries(steps).forEach(([step, val]) => {
                merged[col][step] = val;
            });
        });
        const res = await fetch("/api/save_interlock_limits", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(merged)
        });
        if (res.ok) {
            interlockLimits = merged;
            // 모달을 닫지 않고 유지
            fetchData();
        } else {
            alert("저장 실패");
        }
    }

    async function loadInterlockLimits() {
        try {
            const res = await fetch('/api/interlock_limits');
            if (!res.ok) return;
            const json = await res.json();
            interlockLimits = json.limits || {};
        } catch (e) {
            console.error('failed to load interlock limits', e);
        }
    }

    document.getElementById("open-interlock-settings")?.addEventListener("click", () => {
        createInterlockSettingsUI();
        document.getElementById("interlock-settings-modal").style.display = "block";
    });

    document.getElementById("save-interlock-settings")?.addEventListener("click", saveInterlockLimits);

    loadLimits();
    loadInterlockLimits();

    document.querySelectorAll('.expand-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            openChartModal(btn.dataset.param, btn.dataset.kind);
        });
    });

    // 시간 범위 선택 이벤트 핸들러
    const timeRangeSelect = document.getElementById('time-range-select');
    if (timeRangeSelect) {
        timeRangeSelect.addEventListener('change', (e) => {
            selectedTimeRange = parseInt(e.target.value, 10);
            // 차트 데이터 즉시 갱신 (HOLD가 꺼져있을 때만)
            if (!chartHoldEnabled) {
                fetchData();
            }
        });
    }

    // HOLD 토글 버튼 이벤트 핸들러
    const chartHoldToggle = document.getElementById('chart-hold-toggle');
    if (chartHoldToggle) {
        chartHoldToggle.addEventListener('change', (e) => {
            chartHoldEnabled = e.target.checked;
        });
    }

    // 모달 HOLD 토글 버튼 이벤트 핸들러
    const modalChartHoldToggle = document.getElementById('modal-chart-hold-toggle');
    if (modalChartHoldToggle) {
        modalChartHoldToggle.addEventListener('change', (e) => {
            chartHoldEnabled = e.target.checked;
            // 메인 HOLD 토글도 동기화
            if (chartHoldToggle) {
                chartHoldToggle.checked = e.target.checked;
            }
        });
    }

    // 메인 HOLD 토글 변경 시 모달 HOLD 토글도 동기화
    if (chartHoldToggle) {
        chartHoldToggle.addEventListener('change', (e) => {
            if (modalChartHoldToggle) {
                modalChartHoldToggle.checked = e.target.checked;
            }
        });
    }

    const modal = document.getElementById('chart-modal');
    const closeBtn = document.getElementById('close-modal');
    if (closeBtn) closeBtn.addEventListener('click', closeChartModal);
    if (modal) {
        modal.addEventListener('click', (e) => {
            if (e.target === modal) closeChartModal();
        });
    }

    warningElements = {
        warningModal: document.getElementById('warning-modal'),
        warningParam: document.getElementById('warning-param'),
        warningClose: document.getElementById('warning-close'),
        warningCancel: document.getElementById('warning-cancel'),
        warningDown: document.getElementById('warning-down'),
        confirmModal: document.getElementById('confirm-modal'),
        confirmClose: document.getElementById('confirm-close'),
        confirmNo: document.getElementById('confirm-no'),
        confirmYes: document.getElementById('confirm-yes')
    };
    reportElements = {
        reportModal: document.getElementById('report-modal'),
        reportClose: document.getElementById('report-close'),
        reportSummary: document.getElementById('report-summary'),
        reportBody: document.getElementById('report-body')
    };

    if (warningElements.warningClose) warningElements.warningClose.addEventListener('click', closeWarningModal);
    if (warningElements.warningCancel) warningElements.warningCancel.addEventListener('click', closeWarningModal);
    if (warningElements.warningDown) warningElements.warningDown.addEventListener('click', openConfirmModal);
    if (warningElements.confirmClose) warningElements.confirmClose.addEventListener('click', closeConfirmModal);
    if (warningElements.confirmNo) warningElements.confirmNo.addEventListener('click', closeConfirmModal);
    if (warningElements.confirmYes) warningElements.confirmYes.addEventListener('click', () => {
        closeConfirmModal();
        closeWarningModal();
        stopEquipment(); // 경고팝업창의 DOWN버튼 클릭 시 장비 중지
    });
    if (reportElements.reportClose) reportElements.reportClose.addEventListener('click', closeReportModal);
    if (reportElements.reportModal) {
        reportElements.reportModal.addEventListener('click', (e) => {
            if (e.target === reportElements.reportModal) closeReportModal();
        });
    }
    document.querySelectorAll('.report-tab').forEach(tab => {
        tab.addEventListener('click', () => setActiveReportTab(tab.dataset.tab));
    });

    // 경고 팝업 토글
    const warningToggle = document.getElementById('warning-toggle');
    const stored = localStorage.getItem(warningToggleKey);
    if (stored !== null) warningEnabled = stored === 'true';
    if (warningToggle) warningToggle.checked = warningEnabled;
    if (warningToggle) {
        warningToggle.addEventListener('change', () => {
            warningEnabled = warningToggle.checked;
            localStorage.setItem(warningToggleKey, String(warningEnabled));
            saveWarningSetting();
            if (!warningEnabled && warningModalOpen) closeWarningModal();
            // 경고팝업토글 변경 시 차트 업데이트하여 상한선/하한선 표시 상태 변경
            // 모든 차트의 상한선/하한선 표시 상태를 즉시 업데이트
            Object.keys(mainCharts).forEach(col => {
                const chart = mainCharts[col];
                if (!chart) return;
                if (warningEnabled && limits && limits[col]) {
                    // limits가 있으면 상한선/하한선 데이터를 다시 계산하여 표시
                    const actual = chart.data.datasets[1].data || [];
                    const predicted = chart.data.datasets[0].data || [];
                    const allData = actual.concat(predicted);

                    let timePoints = [];
                    if (allData.length > 0) {
                        timePoints = allData.map(d => d.x);
                    } else {
                        const now = Date.now();
                        const timeRangeMs = selectedTimeRange * 60 * 1000;
                        const startTime = now - timeRangeMs;
                        timePoints = [new Date(startTime), new Date(now)];
                    }

                    let currentStep = 'all';
                    if (actual.length > 0 && actual[actual.length - 1].step_id !== undefined) {
                        currentStep = actual[actual.length - 1].step_id?.toString() || 'all';
                    } else if (predicted.length > 0 && predicted[predicted.length - 1].step_id !== undefined) {
                        currentStep = predicted[predicted.length - 1].step_id?.toString() || 'all';
                    }
                    const limit = limits[col][currentStep] || limits[col]['all'];

                    const upperLimit = [];
                    const lowerLimit = [];
                    if (limit && (limit.max !== undefined || limit.min !== undefined)) {
                        timePoints.forEach(x => {
                            if (limit.max !== undefined && limit.max !== null) {
                                upperLimit.push({ x: x, y: limit.max });
                            }
                            if (limit.min !== undefined && limit.min !== null) {
                                lowerLimit.push({ x: x, y: limit.min });
                            }
                        });
                    }

                    chart.data.datasets[2].data = upperLimit;
                    chart.data.datasets[3].data = lowerLimit;
                    chart.data.datasets[2].hidden = !warningEnabled || upperLimit.length === 0;
                    chart.data.datasets[3].hidden = !warningEnabled || lowerLimit.length === 0;
                } else {
                    // limits가 없거나 warningEnabled가 false면 숨김
                    chart.data.datasets[2].data = [];
                    chart.data.datasets[3].data = [];
                    chart.data.datasets[2].hidden = true;
                    chart.data.datasets[3].hidden = true;
                }

                // Interlock 상한선/하한선 업데이트
                if (warningEnabled && interlockLimits && interlockLimits[col]) {
                    const interlockLimit = interlockLimits[col]['all'];
                    const interlockUpperLimit = [];
                    const interlockLowerLimit = [];

                    if (interlockLimit && (interlockLimit.max !== undefined || interlockLimit.min !== undefined)) {
                        timePoints.forEach(x => {
                            if (interlockLimit.max !== undefined && interlockLimit.max !== null) {
                                interlockUpperLimit.push({ x: x, y: interlockLimit.max });
                            }
                            if (interlockLimit.min !== undefined && interlockLimit.min !== null) {
                                interlockLowerLimit.push({ x: x, y: interlockLimit.min });
                            }
                        });
                    }

                    chart.data.datasets[4].data = interlockUpperLimit;
                    chart.data.datasets[5].data = interlockLowerLimit;
                    chart.data.datasets[4].hidden = !warningEnabled || interlockUpperLimit.length === 0;
                    chart.data.datasets[5].hidden = !warningEnabled || interlockLowerLimit.length === 0;
                } else {
                    chart.data.datasets[4].data = [];
                    chart.data.datasets[5].data = [];
                    chart.data.datasets[4].hidden = true;
                    chart.data.datasets[5].hidden = true;
                }

                chart.update();
            });
        });
    }

    // 장비 상태 RUN/DOWN 버튼 클릭 이벤트
    const deviceRunBtn = document.getElementById('device-state-run');
    const deviceDownBtn = document.getElementById('device-state-down');

    async function startEquipment() {
        try {
            const res = await fetch('/api/equipment/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await res.json();
            if (res.ok) {
                if (data.status === 'already_running') {
                    alert('⚠️ ' + data.message);
                } else if (data.status === 'partially_running') {
                    alert('⚠️ ' + data.message);
                } else {
                    const started = data.started || [];
                    if (started.length > 0) {
                        alert(`✅ 장비가 시작되었습니다.\n시작된 프로세스: ${started.join(', ')}`);
                    } else {
                        alert('✅ ' + data.message);
                    }
                }
                // 장비 시작 시 시작 시간 업데이트 (2분 그레이스 기간 재설정)
                if (data.status === 'started' || data.status === 'partially_running') {
                    pageStartTime = Date.now();
                    console.log('[장비 시작] 그레이스 기간 재설정됨');
                }
                // 상태 업데이트
                pollGeneratorStatus();
            } else {
                alert('❌ 오류: ' + (data.message || '프로세스 시작 실패'));
            }
        } catch (e) {
            console.error('Start equipment error:', e);
            alert('❌ 오류: ' + e.message);
        }
    }

    async function stopEquipment() {
        try {
            const res = await fetch('/api/equipment/stop', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await res.json();
            if (res.ok) {
                if (data.status === 'already_stopped') {
                    alert('⚠️ ' + data.message);
                } else if (data.status === 'stopped') {
                    // 장비가 성공적으로 DOWN되었을 때 텔레그램 알림 전송
                    await sendTelegramNotification('🔴 장비가 DOWN 되었습니다.');

                    const killedCount = data.killed_count || 0;
                    if (killedCount > 0) {
                        alert(`✅ 장비가 정지되었습니다.\n종료된 프로세스: ${killedCount}개`);
                    } else {
                        alert('✅ ' + (data.message || '프로세스가 종료되었습니다.'));
                    }
                } else {
                    const killedCount = data.killed_count || 0;
                    if (killedCount > 0) {
                        alert(`✅ 장비가 정지되었습니다.\n종료된 프로세스: ${killedCount}개`);
                    } else {
                        alert('✅ ' + (data.message || '프로세스가 종료되었습니다.'));
                    }
                }
                // 상태 업데이트
                pollGeneratorStatus();
            } else {
                alert('❌ 오류: ' + (data.message || '프로세스 종료 실패'));
            }
        } catch (e) {
            console.error('Stop equipment error:', e);
            alert('❌ 오류: ' + e.message);
        }
    }

    if (deviceRunBtn) {
        deviceRunBtn.style.cursor = 'pointer';
        deviceRunBtn.addEventListener('click', startEquipment);
    }
    if (deviceDownBtn) {
        deviceDownBtn.style.cursor = 'pointer';
        deviceDownBtn.addEventListener('click', () => {
            // 장비상태의 DOWN버튼 클릭 시 확인 메시지 표시
            if (confirm('장비를 Down 하시겠습니까?\n\n⚠️ 주의: 장비 Down은 즉시 실행되며 되돌릴 수 없습니다.')) {
                stopEquipment();
            }
        });
    }

    async function sendTelegramNotification(message) {
        try {
            // 텔레그램 알림을 위한 API 호출 (백엔드에서 처리)
            await fetch('/api/telegram/notify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: message })
            });
        } catch (e) {
            console.error('Telegram notification error:', e);
            // 텔레그램 전송 실패는 무시 (사용자에게는 알리지 않음)
        }
    }
});

window.addEventListener('resize', updateLogPanelHeight);
window.addEventListener('resize', () => {
    if (modalChart) modalChart.resize();
    adjustPartsTableHeight();
});
