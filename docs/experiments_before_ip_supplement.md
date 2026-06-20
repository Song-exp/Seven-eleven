# 실험 결과 — IP-IP 엣지 추가 이전 네트워크 기준

> **기록 시점**: 2026-06-19  
> **네트워크 구성**: product / keyword / ip 노드 + product-keyword / ip-keyword / trend-keyword / product-ip 엣지 (ip-ip 엣지 미포함)  
> **비교 목적**: IP-IP 엣지 추가(`ip_ip_edges_final.parquet`) 후 재실행 결과와의 전후 비교용 베이스라인  
> **복원 방법**: `git show 755b59e^:experiments/results/<exp>/metrics.json`

---

## 지표 설명

| 지표 | 설명 |
|---|---|
| `pr_auc` | Precision-Recall AUC. 클래스 불균형(성공:실패 ≈ 1:3) 환경의 주 평가 지표 |
| `auc_roc` | ROC AUC |
| `f1` | val/test PR-AUC 최적 threshold 기준 F1 |
| `threshold` | 위 F1을 최대화하는 로짓 threshold |

---

## 전체 실험 결과 (test 기준 PR-AUC 정렬)

| 실험 | 핵심 변경점 | test PR-AUC | test ROC-AUC | test F1 |
|---|---|---|---|---|
| exp27_no_sim_1layer | sim 엣지 제거 1홉 애블레이션 | **0.6906** | 0.8693 | 0.6649 |
| exp21_h64_on18 | exp18 + hidden 64 | 0.6867 | 0.8694 | 0.6510 |
| exp22_2hop_kw3 | sim_kw≥3, sim_ip≥2 (★확정 베스트) | 0.6744 | 0.8649 | 0.6650 |
| exp31_2hop_kw3_ip1 | exp22 + IP 임계 2→1 | 0.6753 | 0.8636 | 0.6667 |
| exp23_2hop_kw8 | sim_kw≥8 (희소) | 0.6755 | 0.8605 | 0.6525 |
| exp10_learned_hop_readout | H¹/H² learned readout | 0.6684 | 0.8560 | 0.6545 |
| exp18_multihop_1attn | 멀티홉 1-어텐션 기준 | 0.6681 | 0.8596 | 0.6421 |
| exp15_pos_weight | exp10 + pos_weight 4.5 | 0.6679 | 0.8558 | 0.6385 |
| exp16_hidden64 | exp10 + hidden 64 | 0.6625 | 0.8542 | 0.6419 |
| exp06_both_copurchase | offline+quick 동반구매 | 0.6598 | 0.8294 | 0.6059 |
| exp20_posw_on18 | exp18 + pos_weight | 0.6648 | 0.8614 | 0.6560 |
| exp25_multihop_2layer | exp18 2층 스택 | 0.6594 | 0.8626 | 0.6413 |
| exp08_idf_keyword | IDF 키워드 가중, L=2 | 0.6557 | 0.8514 | 0.6341 |
| exp09_idf_L3 | IDF 키워드 가중, L=3 | 0.6487 | 0.8463 | 0.6276 |
| exp07_copurchase_binary | 동반구매 binary (IDF 없음) | 0.6354 | 0.8437 | 0.6387 |
| exp19_reg_on18 | exp18 + 정규화 | 0.5991 | 0.8268 | 0.6176 |
| exp11_no_basket_L1_hop_readout | 동반구매 제거 L1 | 0.5786 | 0.8223 | 0.6098 |
| exp12_no_basket_L2_hop_readout | 동반구매 제거 L2 | 0.5680 | 0.8237 | 0.6169 |
| exp14_regularized | exp10 + 정규화 | 0.5586 | 0.8079 | 0.5875 |
| exp13_no_basket_L3_hop_readout | 동반구매 제거 L3 | 0.5484 | 0.8093 | 0.5856 |
| exp03_complement_edges | 보완재 엣지 추가 | 0.5464 | 0.7883 | 0.5529 |
| exp02_alpha_tuning | α lr 조정 | 0.5275 | 0.7699 | 0.5316 |
| exp01_baseline | 베이스라인 | 0.5265 | 0.7725 | 0.5333 |

> **미완료**: exp04 / exp05 (미실행), exp24 (OOM), exp28~30 (dense A² — 결과 미저장)

---

## 실험별 상세 지표

### exp01 — Baseline
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.5953 | 0.8169 | 0.5711 | 0.6149 |
| val   | 0.5043 | 0.7456 | 0.5000 | 0.3260 |
| test  | 0.5265 | 0.7725 | 0.5333 | 0.3889 |

### exp02 — Alpha Tuning
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.5728 | 0.8030 | 0.5646 | 0.4013 |
| val   | 0.4962 | 0.7400 | 0.5116 | 0.5772 |
| test  | 0.5275 | 0.7699 | 0.5316 | 0.4001 |

### exp03 — Complement Edges
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7510 | 0.9203 | 0.7247 | 0.6482 |
| val   | 0.5138 | 0.7663 | 0.5308 | 0.5710 |
| test  | 0.5464 | 0.7883 | 0.5529 | 0.7097 |

### exp06 — Both Copurchase
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7743 | 0.9319 | 0.7539 | 0.6633 |
| val   | 0.6396 | 0.8123 | 0.6154 | 0.7508 |
| test  | 0.6598 | 0.8294 | 0.6059 | 0.7804 |

### exp07 — Copurchase Binary
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7665 | 0.9125 | 0.7306 | 0.4774 |
| val   | 0.7301 | 0.8503 | 0.6548 | 0.8297 |
| test  | 0.6354 | 0.8437 | 0.6387 | 0.6731 |

### exp08 — IDF Keyword (L=2)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7957 | 0.9361 | 0.7876 | 0.7787 |
| val   | 0.7166 | 0.8514 | 0.6686 | 0.8855 |
| test  | 0.6557 | 0.8514 | 0.6341 | 0.7713 |

### exp09 — IDF Keyword (L=3)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7993 | 0.9321 | 0.7588 | 0.5751 |
| val   | 0.7292 | 0.8523 | 0.6628 | 0.8224 |
| test  | 0.6487 | 0.8463 | 0.6276 | 0.6542 |

### exp10 — Learned Hop Readout
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8064 | 0.9435 | 0.7822 | 0.8422 |
| val   | 0.7381 | 0.8633 | 0.6720 | 0.8469 |
| test  | 0.6684 | 0.8560 | 0.6545 | 0.8665 |

### exp11 — No Basket L1 Hop Readout
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8068 | 0.9452 | 0.7730 | 0.7415 |
| val   | 0.5917 | 0.8151 | 0.5930 | 0.5378 |
| test  | 0.5786 | 0.8223 | 0.6098 | 0.6186 |

### exp12 — No Basket L2 Hop Readout
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8065 | 0.9389 | 0.7501 | 0.6255 |
| val   | 0.5712 | 0.8049 | 0.5956 | 0.5646 |
| test  | 0.5680 | 0.8237 | 0.6169 | 0.6370 |

### exp13 — No Basket L3 Hop Readout
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.9285 | 0.9821 | 0.8881 | 0.8222 |
| val   | 0.5994 | 0.8166 | 0.6051 | 0.2620 |
| test  | 0.5484 | 0.8093 | 0.5856 | 0.7544 |

### exp14 — Regularized (on exp10)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.5850 | 0.8429 | 0.6099 | 0.6418 |
| val   | 0.5429 | 0.7844 | 0.5374 | 0.6681 |
| test  | 0.5586 | 0.8079 | 0.5875 | 0.6679 |

### exp15 — Pos Weight (on exp10)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8221 | 0.9503 | 0.7936 | 0.6727 |
| val   | 0.7419 | 0.8666 | 0.6739 | 0.8453 |
| test  | 0.6679 | 0.8558 | 0.6385 | 0.7560 |

### exp16 — Hidden 64 (on exp10)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8082 | 0.9496 | 0.8016 | 0.5862 |
| val   | 0.7281 | 0.8594 | 0.6686 | 0.8099 |
| test  | 0.6625 | 0.8542 | 0.6419 | 0.7835 |

### exp18 — Multihop 1-Attention
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8171 | 0.9476 | 0.7820 | 0.7302 |
| val   | 0.7378 | 0.8692 | 0.6667 | 0.8153 |
| test  | 0.6681 | 0.8596 | 0.6421 | 0.8262 |

### exp19 — Regularized (on exp18)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7842 | 0.9301 | 0.7284 | 0.4716 |
| val   | 0.5755 | 0.8024 | 0.5744 | 0.4518 |
| test  | 0.5991 | 0.8268 | 0.6176 | 0.4494 |

### exp20 — Pos Weight (on exp18)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7720 | 0.9277 | 0.7353 | 0.7673 |
| val   | 0.7435 | 0.8659 | 0.6569 | 0.8428 |
| test  | 0.6648 | 0.8614 | 0.6560 | 0.8252 |

### exp21 — Hidden 64 (on exp18)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7801 | 0.9309 | 0.7465 | 0.6275 |
| val   | 0.7366 | 0.8717 | 0.6650 | 0.6192 |
| test  | 0.6867 | 0.8694 | 0.6510 | 0.8176 |

### exp22 — 2hop kw≥3 ★ 확정 최종 모델 (이전 네트워크 기준)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8375 | 0.9551 | 0.8075 | 0.7951 |
| val   | 0.7482 | 0.8684 | 0.6688 | 0.8787 |
| test  | 0.6744 | 0.8649 | 0.6650 | 0.7307 |

### exp23 — 2hop kw≥8
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8051 | 0.9436 | 0.7738 | 0.7820 |
| val   | 0.7461 | 0.8690 | 0.6631 | 0.7932 |
| test  | 0.6755 | 0.8605 | 0.6525 | 0.8072 |

### exp25 — Multihop 2-Layer Stack
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7869 | 0.9345 | 0.7504 | 0.6966 |
| val   | 0.7337 | 0.8661 | 0.6530 | 0.6036 |
| test  | 0.6594 | 0.8626 | 0.6413 | 0.8123 |

### exp27 — No Sim 1-Layer (애블레이션)
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.7756 | 0.9301 | 0.7367 | 0.6220 |
| val   | 0.7333 | 0.8666 | 0.6649 | 0.6659 |
| test  | 0.6906 | 0.8693 | 0.6649 | 0.7185 |

### exp31 — 2hop kw≥3 + IP 임계 2→1
| split | pr_auc | auc_roc | f1 | threshold |
|---|---|---|---|---|
| train | 0.8102 | 0.9465 | 0.7856 | 0.5959 |
| val   | 0.7306 | 0.8637 | 0.6723 | 0.7584 |
| test  | 0.6753 | 0.8636 | 0.6667 | 0.7705 |

---

## 미완료 실험

| 실험 | 이유 |
|---|---|
| exp04_offline_copurchase | 미실행 (config만 존재) |
| exp05_quick_copurchase | 미실행 (config만 존재) |
| exp24_2hop_kw2 | OOM — sim_kw≥2 엣지 밀도 과다 |
| exp28~30 (dense A²) | methodB 노트북 내 구현, 결과 미저장 |
