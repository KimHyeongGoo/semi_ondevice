import os, glob
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

class SimpleRAG:
    def __init__(self, pdf_dir="data/pdf", index_dir="data/vectordb", model_name="sentence-transformers/all-MiniLM-L6-v2"):
        os.makedirs(index_dir, exist_ok=True)
        self.pdf_dir = pdf_dir
        self.index_path = os.path.join(index_dir, "faiss.index")
        self.meta_path  = os.path.join(index_dir, "meta.npy")
        self.texts_path = os.path.join(index_dir, "texts.npy")
        self.model = SentenceTransformer(model_name)
        self.index = None
        self.texts = []
        self.meta  = []

    def _chunker(self, text, maxlen=800):
        words = text.split()
        buf, cur = [], 0
        for w in words:
            buf.append(w); cur += len(w)+1
            if cur >= maxlen:
                yield " ".join(buf); buf=[]; cur=0
        if buf: yield " ".join(buf)

    def build(self):
        texts, meta = [], []
        for pdf in glob.glob(os.path.join(self.pdf_dir, "*.pdf")):
            reader = PdfReader(pdf)
            for i, page in enumerate(reader.pages):
                t = page.extract_text() or ""
                for ch in self._chunker(t):
                    texts.append(ch)
                    meta.append({"source": os.path.basename(pdf), "page": i+1})
        if not texts:
            self.index = faiss.IndexFlatIP(384)  # for MiniLM; adjust for your model
            self.texts, self.meta = [], []
            return
        embs = self.model.encode(texts, normalize_embeddings=True, batch_size=64)
        dim = embs.shape[1]
        index = faiss.IndexFlatIP(dim)
        index.add(embs.astype(np.float32))
        faiss.write_index(index, self.index_path)
        np.save(self.meta_path, np.array(meta, dtype=object))
        np.save(self.texts_path, np.array(texts, dtype=object))
        self.index, self.texts, self.meta = index, texts, meta

    def load(self):
        if os.path.exists(self.index_path):
            self.index = faiss.read_index(self.index_path)
            self.meta  = np.load(self.meta_path, allow_pickle=True).tolist()
            self.texts = np.load(self.texts_path, allow_pickle=True).tolist()
        else:
            self.build()

    def search(self, query, topk=5):
        if self.index is None: self.load()
        if len(self.texts)==0: return []
        q = self.model.encode([query], normalize_embeddings=True)
        D, I = self.index.search(q.astype(np.float32), topk)
        out=[]
        for idx, score in zip(I[0], D[0]):
            item = {"text": self.texts[idx], "meta": self.meta[idx], "score": float(score)}
            out.append(item)
        return out
