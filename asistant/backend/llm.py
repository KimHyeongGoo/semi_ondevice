import os, requests

class LLM:
    def __init__(self, provider="ollama", model="qwen2.5:7b-instruct"):
        self.provider = provider
        self.model = model

    def chat(self, prompt):
        if self.provider=="ollama":
            # curl http://localhost:11434/api/generate
            r = requests.post("http://localhost:11434/api/generate", json={"model": self.model, "prompt": prompt, "stream": False}, timeout=120)
            r.raise_for_status()
            return r.json().get("response","")
        elif self.provider=="openai":
            # OPENAI_API_KEY in env, uses responses API (pseudo)
            import openai
            openai.api_key = os.getenv("OPENAI_API_KEY")
            resp = openai.chat.completions.create(
                model=self.model, messages=[{"role":"user","content":prompt}], temperature=0.2
            )
            return resp.choices[0].message.content
        else:
            raise NotImplementedError("provider not supported")
