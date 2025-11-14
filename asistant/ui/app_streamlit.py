import streamlit as st, requests, os

BACKEND = os.getenv("BACKEND_URL","http://localhost:8000")

st.set_page_config(page_title="반도체 장비 로그 분석 에이전트", layout="wide")
st.title("반도체 장비 로그 분석 에이전트 (RAG+LLM)")

with st.sidebar:
    st.header("모델 설정")
    provider = st.selectbox("모델 제공자", ["ollama","openai"])
    if provider=="ollama":
        model = st.text_input("Ollama 모델", value="qwen2.5:7b-instruct")
        st.caption("예: qwen2.5:7b-instruct, llama3.1:8b-instruct 등")
    else:
        model = st.text_input("OpenAI 모델", value="gpt-4o-mini")
        st.caption("환경변수 OPENAI_API_KEY 필요")

    st.header("PDF 색인")
    pdf = st.file_uploader("장비/파라미터/조치 매뉴얼 PDF 업로드", type=["pdf"])
    if pdf and st.button("색인 업로드"):
        files = {"file": (pdf.name, pdf, "application/pdf")}
        r = requests.post(f"{BACKEND}/ingest_pdf", files=files, timeout=600)
        st.success(r.json())

st.subheader("질의")
query = st.text_input("질문(예: '이 로그 원인과 조치 알려줘')", "이 로그의 원인과 해결방안은?")
logline = st.text_area("로그 한 줄 붙여넣기", height=120, value=
"2025. 10. 15. 13시 46분 54초 | 기록: 2025. 10. 15. 13시 46분 55초\n컬럼: Baratron Gauge (baratron_gauge_i)\n상태=ON, 세트=2, 위반지속=4.00s :: Baratron Gauge(baratron_gauge_i) 값 4.80000, 기준 5.80000, 편차 17.24%, 허용±10.00%")

if st.button("분석하기", type="primary"):
    payload = {"query": query, "logline": logline, "provider": provider, "model": model}
    r = requests.post(f"{BACKEND}/analyze", json=payload, timeout=180)
    js = r.json()
    st.markdown("### 결과")
    st.write(js["answer"])
    with st.expander("파싱 결과(디버그)"):
        st.json(js["parsed"])
    with st.expander("RAG 검색 컨텍스트(디버그)"):
        st.json(js["ctx"])
