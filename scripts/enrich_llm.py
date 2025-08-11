#!/usr/bin/env python3
import os
import json
import argparse
import pandas as pd
from pathlib import Path
from functools import lru_cache
from sentence_transformers import SentenceTransformer, util
from typing import List

try:
    from transformers import pipeline
except Exception:
    pipeline = None  # Only needed for --mode flan

CITY_CANON = [
    "Tel Aviv-Yafo",
    "Jerusalem",
    "Haifa",
    "Herzliya",
    "Ra'anana",
    "Beer Sheva",
    "Netanya",
    "Ashdod",
    "Ashkelon",
    "Rishon LeZion",
    "Petah Tikva",
    "Other",
]

TITLE_CANON = [
    "Data Scientist",
    "Machine Learning Engineer",
    "AI Engineer",
    "Data Analyst",
    "Data Engineer",
    "Data Architect",
    "Research Scientist",
    "Data Science Manager",
    "Bioinformatics Scientist",
    "Other",
]

DEFAULT_EMBED_MODEL = os.environ.get("HF_SENTENCE_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
DEFAULT_LLM = os.environ.get("HF_T2T_MODEL", "google/flan-t5-small")


def normalize_strings_embed(values: List[str], canon_list: List[str], model_name: str, threshold: float) -> List[str]:
    model = SentenceTransformer(model_name)
    emb_canon = model.encode(canon_list, convert_to_tensor=True, normalize_embeddings=True)
    out = []
    for v in values:
        txt = (v or "").strip()
        if not txt:
            out.append("")
            continue
        emb = model.encode([txt], convert_to_tensor=True, normalize_embeddings=True)
        sim = util.cos_sim(emb, emb_canon)[0]
        idx = int(sim.argmax())
        score = float(sim[idx])
        out.append(canon_list[idx] if score >= threshold else txt)
    return out


@lru_cache(maxsize=8192)
def _classify_once(llm_name: str, prompt: str) -> str:
    if pipeline is None:
        raise RuntimeError("transformers is not available; install to use --mode flan")
    gen = pipeline("text2text-generation", model=llm_name, device=-1)
    resp = gen(prompt, max_new_tokens=8)
    text = (resp[0]["generated_text"] or "").strip()
    return text


def normalize_strings_flan(values: List[str], canon_list: List[str], llm_name: str) -> List[str]:
    labels = ", ".join(canon_list)
    out = []
    for v in values:
        txt = (v or "").strip()
        if not txt:
            out.append("")
            continue
        prompt = (
            "You are a strict classifier. "
            f"Choose exactly one label from this list: [{labels}].\n"
            "Only output the label text, no punctuation, no extra words.\n"
            f"Input: {txt}"
        )
        pred = _classify_once(llm_name, prompt)
        # Guardrail: if model outputs unknown label, keep original
        pred_norm = pred.strip()
        out.append(pred_norm if pred_norm in canon_list else txt)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/jobs.csv")
    ap.add_argument("--output", default="data/jobs_enriched.csv")
    ap.add_argument("--threshold", type=float, default=0.55)
    ap.add_argument("--mode", choices=["embed", "flan"], default="embed", help="embed: ST nearest-neighbor; flan: local FLAN-T5 classifier")
    args = ap.parse_args()

    p = Path(args.input)
    if not p.exists():
        print(f"Input not found: {p}")
        return
    df = pd.read_csv(p)

    locations = df.get("location", pd.Series([""] * len(df))).fillna("").astype(str).tolist()
    titles = df.get("job_title", pd.Series([""] * len(df))).fillna("").astype(str).tolist()

    if args.mode == "embed":
        city_norm = normalize_strings_embed(locations, CITY_CANON, DEFAULT_EMBED_MODEL, threshold=args.threshold)
        title_norm = normalize_strings_embed(titles, TITLE_CANON, DEFAULT_EMBED_MODEL, threshold=args.threshold)
    else:
        city_norm = normalize_strings_flan(locations, CITY_CANON, DEFAULT_LLM)
        title_norm = normalize_strings_flan(titles, TITLE_CANON, DEFAULT_LLM)

    df["city_normalized"] = city_norm
    df["title_normalized"] = title_norm

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False)
    print(f"Wrote {outp} with {len(df)} rows using mode={args.mode}")


if __name__ == "__main__":
    main() 