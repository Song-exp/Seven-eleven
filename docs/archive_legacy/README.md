# archive_legacy — 구버전/폐기 문서 보관

> ⚠️ **이 폴더의 문서는 현행이 아니다.** 폐기된 **exp41(동반구매 누수 포함)** 모델 기준으로 작성되어 실제 운영(현행 서빙 **v2_sweepA**)과 수치·결론이 다르다.
> 내용은 이력 보존용으로 원형 유지하되, 의사결정·인용에는 사용하지 말 것.

| 파일 | 원래 역할 | 현행 대체 문서 |
|---|---|---|
| `final_model_summary.md` | exp41 모델 상세 스펙(PR-AUC 0.6959 등) | [`final_model_summary` 대체 → `final_evolution_report.md`](../final_evolution_report.md) · [`final_model_leakfree_switch_plan.md`](../final_model_leakfree_switch_plan.md) |
| `eda_guide_final_model.md` | exp41 모델 EDA 실행 가이드(팀원용) | [`md_prescription_system_guide.md`](../md_prescription_system_guide.md) · [`eda_channel_prescription_plan.md`](../eda_channel_prescription_plan.md) |

> **왜 exp41이 폐기됐나**: 동반구매 엣지의 target leakage로 성능이 과대평가됨(차수 24배 차이). 누수 제거 후 exp47 → v2_sweepA로 전환. 상세: `../final_model_leakfree_switch_plan.md`.
>
> 참고: `hin_gnn_results.md`는 `export_results.py`가 자동 생성하는 산출물이라 archive 대상이 아니며 `docs/`에 그대로 둔다(모델 재export 시 갱신됨).
