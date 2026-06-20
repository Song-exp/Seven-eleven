"""키워드 확정 마스터 CSV 생성 → data/processed/hin/keyword_final.csv.

실행: python -m src.eval.md.export_keyword_final [exp47|v2_sweepA]
이후 CSV에서 include/tag 손보고 → python -m scripts.export_dashboard → 대시보드 반영.
"""
import sys

from .engine import MDEngine, EngineConfig
from .inspector import export_keyword_final


def main():
    model = sys.argv[1] if len(sys.argv) > 1 else "exp47"
    cfg = EngineConfig.exp47() if model == "exp47" else EngineConfig.v2_sweepA()
    eng = MDEngine(cfg).run_single_inference()
    eng.build_mass()
    eng.build_ledger("full")
    df = export_keyword_final(eng)
    tagged = df[df.tag != "neutral"]
    print(f"saved: data/processed/hin/keyword_final.csv  ({len(df)} 키워드 / 태그 {len(tagged)} / model={model})")
    print("\n태그 키워드 추천액션 분포:")
    print(tagged["suggested"].value_counts().to_string())
    print("\n샘플 (killer):")
    cols = ["keyword", "tag", "성공률", "purity", "delta_prob_mean", "매출중앙값", "suggested"]
    print(tagged[tagged.tag == "killer"][cols].head(12).to_string(index=False))


if __name__ == "__main__":
    main()
