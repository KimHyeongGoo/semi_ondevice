def build_prompt(context_chunks, parsed):
    ctx = "\n\n".join([f"[{i+1}] ({c['meta']['source']} p{c['meta']['page']})\n{c['text']}" for i,c in enumerate(context_chunks)])
    p = f"""
[시스템]
너는 반도체 증착 PVD 장비 이상 분석 전문가다. 아래 "RAG_컨텍스트"와 "정규화_로그"만을 근거로
1) 로그분석, 2) 원인분석(가능성 높은 순 Top-N, 각 근거 포함), 3) 해결방안(즉시/근본조치 분리)을 한국어로 간결히 작성하라.

[RAG_컨텍스트]
{ctx if ctx else "(문서 없음)"}

[정규화_로그]
{parsed}

[출력형식]
# 로그분석
...
# 원인분석(우선순위)
1) ...
# 해결방안(즉시/근본)
- 즉시: ...
- 근본: ...
"""
    return p
