"""exp28 — Dense A^2 Shared-Attention Hadamard 구조 + OOM Crash Test.

목적:
  2홉 지름길 엣지(sim_kw/sim_ip)를 따로 생성하지 않고, 전용 레이어도 안 두고,
  단일 어텐션 층 안에서 1홉(co-purchase)과 2홉(순수 A^2) 인접행렬을 가중합한 M에
  공유 어텐션 스코어 행렬 S 를 원소별 곱(Hadamard)하는 구조를 검증.

제약(crash test):
  CPU 전처리에서 A^2 에 어떤 임계값 컷오프도 걸지 않음(No Threshold Cut).
  순수 dense A^2 (P×P) 성분을 그대로 유지하여 GPU 로 토스 → OOM 여부 실측.

수식 (제품 공간, head h):
  H_p = content_aggregation(keyword/ip emb) + promo feat        # (P,d)
  Z[h,p,q]   = (q_p · k_q)/sqrt(d_k)                            # (H,P,P) 공유 어텐션
  M[p,q]     = w1·A1[p,q] + w2·A2[p,q]   (w=softmax(hop_logits))# 1·2홉 인접 가중합
  attn       = softmax_q(Z)                                     # (H,P,P)
  E          = attn ⊙ M   (Hadamard, M broadcast over heads)   # 원소별 곱 = 엣지값
  E          = E / E.sum(q)                                     # 재정규화
  out_p      = Σ_q E[h,p,q] · v_q                               # 집계
  emb        = H_p + a_lin(out)                                 # KGAT식 잔차
  logit      = head(emb)

실행: python -m experiments.exp28_dense_hop2_crashtest
"""
from __future__ import annotations

import math
import traceback

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import average_precision_score, roc_auc_score

from src.data_builder.build_hetero_data import build_graph

HID, HEADS, DROP = 128, 4, 0.3
LR, WD, EPOCHS, PATIENCE = 0.005, 5e-4, 200, 30
POS_W = 3.24
SEED = 42


def _dense_adj(ei: torch.Tensor, n: int, device) -> torch.Tensor:
    """sparse edge_index(2,E) → dense (n,n) float32 인접행렬 (양방향, 대각 0)."""
    A = torch.zeros(n, n, dtype=torch.float32)
    r, c = ei[0], ei[1]
    A[r, c] = 1.0
    A[c, r] = 1.0
    A.fill_diagonal_(0.0)
    return A.to(device)


def _dense_hop2(ei_bipartite: torch.Tensor, n_src: int, n_dst: int, device) -> torch.Tensor:
    """P-X bipartite edge_index → 순수 dense A^2 (n_src,n_src), 임계컷 없음, 대각 0."""
    B = torch.zeros(n_src, n_dst, dtype=torch.float32)
    B[ei_bipartite[0], ei_bipartite[1]] = 1.0
    A2 = B @ B.t()                       # (P,P) 공유 카운트 — DENSE, no cutoff
    A2.fill_diagonal_(0.0)
    return A2.to(device)


class DenseHop2GNN(nn.Module):
    def __init__(self, n_kw, n_ip, hidden=HID, heads=HEADS, dropout=DROP):
        super().__init__()
        self.H, self.dk = heads, hidden // heads
        self.sqrt_dk = math.sqrt(self.dk)
        self.keyword_emb = nn.Embedding(n_kw, hidden)
        self.ip_emb = nn.Embedding(n_ip, hidden)
        nn.init.xavier_uniform_(self.keyword_emb.weight)
        nn.init.xavier_uniform_(self.ip_emb.weight)
        self.product_feat_lin = nn.Linear(2, hidden)
        self.q_lin = nn.Linear(hidden, hidden)
        self.k_lin = nn.Linear(hidden, hidden)
        self.v_lin = nn.Linear(hidden, hidden)
        self.a_lin = nn.Linear(hidden, hidden)
        self.hop_logits = nn.Parameter(torch.zeros(2))   # [1홉, 2홉] 가중 (softmax)
        self.drop = nn.Dropout(dropout)
        self.head = nn.Sequential(
            nn.Linear(hidden, hidden), nn.ReLU(), nn.Dropout(dropout),
            nn.Linear(hidden, 1),
        )
        self._last_hopw = None

    def init_product(self, pk_ei, pi_ei, P, has_promo, im30):
        dev = self.keyword_emb.weight.device
        acc = torch.zeros(P, self.keyword_emb.embedding_dim, device=dev)
        from torch_geometric.utils import scatter
        km = self.keyword_emb(pk_ei[1])
        acc = acc + scatter(km, pk_ei[0], dim=0, dim_size=P, reduce="mean")
        im = self.ip_emb(pi_ei[1])
        acc = acc + scatter(im, pi_ei[0], dim=0, dim_size=P, reduce="mean")
        feat = torch.stack([has_promo.float(), im30.float()], dim=-1)
        return acc + self.product_feat_lin(feat)

    def forward(self, pk_ei, pi_ei, A1, A2, has_promo, im30):
        P = A1.size(0)
        H, dk = self.H, self.dk
        Hp = self.init_product(pk_ei, pi_ei, P, has_promo, im30)          # (P,d)
        q = self.q_lin(Hp).view(P, H, dk)
        k = self.k_lin(Hp).view(P, H, dk)
        v = self.v_lin(Hp).view(P, H, dk)
        Z = torch.einsum("phd,qhd->hpq", q, k) / self.sqrt_dk            # (H,P,P) ← OOM 후보
        hopw = torch.softmax(self.hop_logits, dim=0)
        self._last_hopw = hopw.detach().cpu()
        M = hopw[0] * A1 + hopw[1] * A2                                  # (P,P) 1·2홉 가중합
        attn = torch.softmax(Z, dim=-1)                                  # (H,P,P)
        E = attn * M.unsqueeze(0)                                        # Hadamard
        E = E / (E.sum(dim=-1, keepdim=True) + 1e-9)
        E = self.drop(E)
        out = torch.einsum("hpq,qhd->phd", E, v).reshape(P, H * dk)      # (P,d)
        emb = Hp + self.a_lin(out)                                       # 잔차
        return self.head(emb).squeeze(-1)


def main():
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    torch.manual_seed(SEED)
    print(f"device={dev}  {torch.cuda.get_device_name(0) if dev.type=='cuda' else ''}")

    # 1홉 co-purchase 포함해서 그래프 로드 (sim 엣지는 안 씀)
    data, maps = build_graph(
        seed=SEED, include_offline_copurchase=True, include_quick_copurchase=True,
        use_lift_weights=False, add_2hop_edges=False,
    )
    P = data["product"].num_nodes
    n_kw, n_ip = data["keyword"].num_nodes, data["ip"].num_nodes
    pk_ei = data["product", "has_kw", "keyword"].edge_index.to(dev)
    pi_ei = data["product", "has_ip", "ip"].edge_index.to(dev)
    y = data["product"].y.to(dev)
    hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    tr = data["product"].train_mask.to(dev)
    va = data["product"].val_mask.to(dev)
    te = data["product"].test_mask.to(dev)

    print(f"P={P} K={n_kw} I={n_ip}")

    # === CPU 전처리: 순수 dense A1(co-purchase 1홉), A2(P-K-P + P-I-P, 임계컷 없음) ===
    print("[CPU] dense A1(co-purchase) / A2(pure, no cutoff) 구성 중...")
    co = []
    for et in [("product", "co_offline", "product"), ("product", "co_quick", "product")]:
        if et in data.edge_types:
            co.append(data[et].edge_index)
    co_ei = torch.cat(co, dim=1) if co else torch.empty(2, 0, dtype=torch.long)
    A1 = _dense_adj(co_ei, P, "cpu")
    A2_kw = _dense_hop2(data["product", "has_kw", "keyword"].edge_index, P, n_kw, "cpu")
    A2_ip = _dense_hop2(data["product", "has_ip", "ip"].edge_index, P, n_ip, "cpu")
    A2 = A2_kw + A2_ip
    # 스케일 정규화 (hop weight 해석 위해 각 [0,1])
    A1 = A1 / (A1.max() + 1e-9)
    A2 = A2 / (A2.max() + 1e-9)
    nz1 = int((A1 > 0).sum()); nz2 = int((A2 > 0).sum())
    print(f"  A1 nnz={nz1:,} ({100*nz1/P/P:.1f}% dense)  A2 nnz={nz2:,} ({100*nz2/P/P:.1f}% dense)")
    print(f"  dense 행렬 메모리: A1+A2 = {2*P*P*4/1e9:.2f}GB (float32)")

    # === GPU 토스 (crash test) ===
    print("[GPU] dense A1, A2 토스 시도...")
    A1 = A1.to(dev); A2 = A2.to(dev)
    if dev.type == "cuda":
        print(f"  토스 후 GPU 점유: {torch.cuda.memory_allocated()/1e9:.2f}GB / "
              f"reserved {torch.cuda.memory_reserved()/1e9:.2f}GB")

    model = DenseHop2GNN(n_kw, n_ip).to(dev)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([POS_W], device=dev))

    def pr_auc(mask):
        model.eval()
        with torch.no_grad():
            logit = model(pk_ei, pi_ei, A1, A2, hp, im30)
            p = torch.sigmoid(logit[mask]).cpu().numpy()
            t = y[mask].cpu().numpy()
        return average_precision_score(t, p), roc_auc_score(t, p)

    best, best_state, wait = -1.0, None, 0
    for ep in range(1, EPOCHS + 1):
        model.train()
        opt.zero_grad()
        logit = model(pk_ei, pi_ei, A1, A2, hp, im30)               # ← dense forward (OOM 후보)
        loss = crit(logit[tr], y[tr])
        loss.backward()                                            # ← dense backward (OOM 후보)
        opt.step()
        vpr, vauc = pr_auc(va)
        if vpr > best:
            best, wait = vpr, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            wait += 1
        if ep % 10 == 0 or ep == 1:
            peak = torch.cuda.max_memory_allocated()/1e9 if dev.type == "cuda" else 0
            print(f"[{ep:03d}] loss={float(loss):.4f} val_pr={vpr:.4f} val_auc={vauc:.4f} "
                  f"hopw(1h/2h)={model._last_hopw.tolist()} peak={peak:.2f}GB")
        if wait >= PATIENCE:
            print(f"early stop @ {ep} (best val_pr={best:.4f})")
            break

    if best_state:
        model.load_state_dict(best_state)
    tpr, tauc = pr_auc(te)
    print("=" * 60)
    print(f"exp28 (dense A^2 shared-attn Hadamard): TEST pr_auc={tpr:.4f} auc_roc={tauc:.4f}")
    print(f"  최종 hop weight (1홉/2홉) = {model._last_hopw.tolist()}")
    if dev.type == "cuda":
        print(f"  peak GPU = {torch.cuda.max_memory_allocated()/1e9:.2f}GB / total 8.5GB")
    print(f"  vs exp27(1홉만) 0.6906 / exp18(sim) 0.6815")


if __name__ == "__main__":
    try:
        main()
    except torch.cuda.OutOfMemoryError as e:
        print("\n" + "=" * 60)
        print("*** CUDA OOM — dense A^2 crash test 결과: 메모리 벽 확인됨 ***")
        print(f"peak allocated = {torch.cuda.max_memory_allocated()/1e9:.2f}GB")
        print(str(e).splitlines()[0])
    except Exception:
        traceback.print_exc()
