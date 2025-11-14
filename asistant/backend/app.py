from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
import os, shutil
from .parsers import parse_log
from .rag import SimpleRAG
from .llm import LLM
from .prompts import build_prompt

app = FastAPI()
RAG = SimpleRAG()

class AnalyzeReq(BaseModel):
    query:str
    logline:str
    provider:str="ollama"     # ollama | openai
    model:str="qwen2.5:7b-instruct"

@app.post("/ingest_pdf")
async def ingest_pdf(file: UploadFile = File(...)):
    os.makedirs("data/pdf", exist_ok=True)
    path = os.path.join("data/pdf", file.filename)
    with open(path,"wb") as f: shutil.copyfileobj(file.file, f)
    RAG.build()
    return {"ok": True, "indexed": file.filename}

@app.post("/analyze")
async def analyze(req:AnalyzeReq):
    parsed = parse_log(req.logline)
    # RAG 검색: 로그의 파라미터명+사용자 쿼리 기반
    q = f"{req.query} {parsed.get('param_name','')} {parsed.get('param_id','')} {parsed.get('state','')}"
    ctx = RAG.search(q, topk=6)
    prompt = build_prompt(ctx, parsed)
    llm = LLM(provider=req.provider, model=req.model)
    answer = llm.chat(prompt)
    return {"answer": answer, "parsed": parsed, "ctx": ctx}
