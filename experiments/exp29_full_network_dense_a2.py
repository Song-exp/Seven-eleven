"""exp29 — 전체 네트워크(N×N) 완전 인접행렬 기반 Dense A^2 Shared-Attention Hadamard.

exp28 정정판:
  A 를 제품공간(P×P)으로 축소하지 않고, product+keyword+ip 전체 노드를 연결한
  완전한 N×N 인접행렬로 둔다 (KGAT A^L 본래 취지). A^2 = A·A 는 전체 네트워크 2홉
  (P-K-K 트렌드, P-I-K, K-K-K 등 모든 경로 포함).

제약(crash test):
  CPU 전처리에서 A^2 에 임계컷 없음. 순수 dense A^2 (N×N) 를 GPU 로 토스.
  N = P+K+I = 7912 → 어텐션 (H,N,N) ≈ 1.0GB/텐서. OOM 여부 실측.

수식 (전체 노드 i,j; head h):
  X[i]     = 노드 초기표현 (product=content agg / keyword,ip=학습 emb)   # (N,d)
  Z[h,i,j] = (q_i · k_j)/sqrt(d_k)   (q/k/v 는 노드타입별 투영 = HGT 격리) # (H,N,N)
  A        = 완전 대칭 인접행렬 (모든 엣지타입 통합)                       # (N,N)
  A2       = A·A   (순수 dense, 임계컷 0, 대각 0)                          # (N,N)
  M        = w1·A + w2·A2   (w=softmax(hop_logits))                       # 1·2홉 가중합
  attn     = softmax_j(Z) ;  E = attn ⊙ M ;  E = E / E.sum(j)            # Hadamard+재정규화
  out_i    = Σ_j E[h,i,j]·v_j                                            # 전체 노드 전파
  emb      = X + a_lin(out) ;  logit = head(emb[:P])                     # product 만 readout

실행: python -m experiments.exp29_full_network_dense_a2
"""
from __future__ import annotations

import math
import traceback

import torch
import torch.nn as nn
from sklearn.metrics import average_precision_score, roc_auc_score
from torch_geometric.utils import scatter

from src.data_builder.build_hetero_data import build_graph

HID, HEADS, DROP = 128, 4, 0.3
LR, WD, EPOCHS, PATIENCE = 0.005, 5e-4, 200, 30
POS_W = 3.24
SEED = 42


class FullNetDenseGNN(nn.Module):
    def __init__(self, P, K, I, hidden=HID, heads=HEADS, dropout=DROP):
        super().__init__()
        self.P, self.K, self.I, self.N = P, K, I, P + K + I
        self.H, self.dk = heads, hidden // heads
        self.sqrt_dk = math.sqrt(self.dk)
        self.keyword_emb = nn.Embedding(K, hidden)
        self.ip_emb = nn.Embedding(I, hidden)
        nn.init.xavier_uniform_(self.keyword_emb.weight)
        nn.init.xavier_uniform_(self.ip_emb.weight)
        self.product_feat_lin = nn.Linear(2, hidden)
        # 노드 타입별 Q/K/V (HGT 타입 격리)
        self.q_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.k_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.v_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.a_lin = nn.Linear(hidden, hidden)
        self.hop_logits = nn.Parameter(torch.zeros(2))
        self.drop = nn.Dropout(dropout)
        self.head = nn.Sequential(
            nn.Linear(hidden, hidden), nn.ReLU(), nn.Dropout(dropout),
            nn.Linear(hidden, 1),
        )
        self._last_hopw = None

    def _init_X(self, pk_ei, pi_ei, has_promo, im30):
        dev = self.keyword_emb.weight.device
        d = self.keyword_emb.embedding_dim
        # product = content aggregation
        prod = torch.zeros(self.P, d, device=dev)
        prod = prod + scatter(self.keyword_emb(pk_ei[1]), pk_ei[0], dim=0, dim_size=self.P, reduce="mean")
        prod = prod + scatter(self.ip_emb(pi_ei[1]), pi_ei[0], dim=0, dim_size=self.P, reduce="mean")
        prod = prod + self.product_feat_lin(torch.stack([has_promo.float(), im30.float()], dim=-1))
        return torch.cat([prod, self.keyword_emb.weight, self.ip_emb.weight], dim=0)  # (N,d)

    def _proj(self, lin_dict, X):
        P, K = self.P, self.K
        out = torch.empty(self.N, self.H, self.dk, device=X.device)
        out[:P] = lin_dict["product"](X[:P]).view(-1, self.H, self.dk)
        out[P:P + K] = lin_dict["keyword"](X[P:P + K]).view(-1, self.H, self.dk)
        out[P + K:] = lin_dict["ip"](X[P + K:]).view(-1, self.H, self.dk)
        return out

    def forward(self, pk_ei, pi_ei, A, A2, has_promo, im30):
        X = self._init_X(pk_ei, pi_ei, has_promo, im30)              # (N,d)
        q = self._proj(self.q_lin, X)
        k = self._proj(self.k_lin, X)
        v = self._proj(self.v_lin, X)
        Z = torch.einsum("nhd,mhd->hnm", q, k) / self.sqrt_dk       # (H,N,N) ← OOM 후보
        hopw = torch.softmax(self.hop_logits, dim=0)
        self._last_hopw = hopw.detach().cpu()
        M = hopw[0] * A + hopw[1] * A2                              # (N,N) 전체망 1·2홉 가중합
        attn = torch.softmax(Z, dim=-1)
        E = attn * M.unsqueeze(0)                                   # Hadamard
        E = E / (E.sum(dim=-1, keepdim=True) + 1e-9)
        E = self.drop(E)
        out = torch.einsum("hnm,mhd->nhd", E, v).reshape(self.N, -1)
        emb = X + self.a_lin(out)
        return self.head(emb[:self.P]).squeeze(-1)                  # product 만


def build_full_adjacency(data, P, K, I, dev):
    """전체 노드 N×N 완전 대칭 인접행렬 + 순수 dense A^2 (임계컷 0)."""
    N = P + K + I
    A = torch.zeros(N, N, dtype=torch.float32)

    def add(ei, off_s, off_t):
        s = ei[0] + off_s
        t = ei[1] + off_t
        A[s, t] = 1.0
        A[t, s] = 1.0

    add(data["product", "has_kw", "keyword"].edge_index, 0, P)            # P-K
    add(data["product", "has_ip", "ip"].edge_index, 0, P + K)            # P-I
    if ("ip", "has_kw", "keyword") in data.edge_types:
        add(data["ip", "has_kw", "keyword"].edge_index, P + K, P)        # I-K
    if ("keyword", "trend_to", "keyword") in data.edge_types:
        add(data["keyword", "trend_to", "keyword"].edge_index, P, P)    # K-K
    for et in [("product", "co_offline", "product"), ("product", "co_quick", "product")]:
        if et in data.edge_types:
            add(data[et].edge_index, 0, 0)                              # P-P
    A.fill_diagonal_(0.0)
    print(f"  A nnz={int((A>0).sum()):,} ({100*(A>0).sum().item()/N/N:.2f}% dense), N={N}")
    A2 = A @ A                                                          # 전체망 dense A^2, no cutoff
    A2.fill_diagonal_(0.0)
    print(f"  A2 nnz={int((A2>0).sum()):,} ({100*(A2>0).sum().item()/N/N:.2f}% dense)")
    A = A / (A.max() + 1e-9)
    A2 = A2 / (A2.max() + 1e-9)
    print(f"  dense 행렬 메모리: A+A2 = {2*N*N*4/1e9:.2f}GB (float32)")
    return A.to(dev), A2.to(dev)


def main():
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    torch.manual_seed(SEED)
    print(f"device={dev}  {torch.cuda.get_device_name(0) if dev.type=='cuda' else ''}")

    data, maps = build_graph(
        seed=SEED, include_offline_copurchase=True, include_quick_copurchase=True,
        use_lift_weights=False, add_2hop_edges=False,
    )
    P = data["product"].num_nodes
    K, I = data["keyword"].num_nodes, data["ip"].num_nodes
    pk_ei = data["product", "has_kw", "keyword"].edge_index.to(dev)
    pi_ei = data["product", "has_ip", "ip"].edge_index.to(dev)
    y = data["product"].y.to(dev)
    hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    tr, va, te = (data["product"].train_mask.to(dev),
                  data["product"].val_mask.to(dev), data["product"].test_mask.to(dev))
    print(f"P={P} K={K} I={I}")

    print("[CPU] 완전 네트워크 A(N×N) + 순수 dense A^2(임계컷 0) 구성 중...")
    A, A2 = build_full_adjacency(data, P, K, I, "cpu")

    print("[GPU] dense A, A2 토스 시도...")
    A = A.to(dev); A2 = A2.to(dev)
    if dev.type == "cuda":
        print(f"  토스 후 GPU 점유: {torch.cuda.memory_allocated()/1e9:.2f}GB")

    model = FullNetDenseGNN(P, K, I).to(dev)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([POS_W], device=dev))

    def pr_auc(mask):
        model.eval()
        with torch.no_grad():
            logit = model(pk_ei, pi_ei, A, A2, hp, im30)
            p = torch.sigmoid(logit[mask]).cpu().numpy()
            t = y[mask].cpu().numpy()
        return average_precision_score(t, p), roc_auc_score(t, p)

    best, best_state, wait = -1.0, None, 0
    for ep in range(1, EPOCHS + 1):
        model.train()
        opt.zero_grad()
        logit = model(pk_ei, pi_ei, A, A2, hp, im30)               # ← full-net dense forward
        loss = crit(logit[tr], y[tr])
        loss.backward()                                            # ← full-net dense backward
        opt.step()
        vpr, vauc = pr_auc(va)
        if vpr > best:
            best, wait = vpr, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            wait += 1
        if ep % 10 == 0 or ep == 1:
            peak = torch.cuda.max_memory_allocated()/1e9 if dev.type == "cuda" else 0
            print(f"[{ep:03d}] loss={float(loss.detach()):.4f} val_pr={vpr:.4f} val_auc={vauc:.4f} "
                  f"hopw(1h/2h)={[round(x,3) for x in model._last_hopw.tolist()]} peak={peak:.2f}GB")
        if wait >= PATIENCE:
            print(f"early stop @ {ep} (best val_pr={best:.4f})")
            break

    if best_state:
        model.load_state_dict(best_state)
    tpr, tauc = pr_auc(te)
    print("=" * 60)
    print(f"exp29 (full-network dense A^2): TEST pr_auc={tpr:.4f} auc_roc={tauc:.4f}")
    print(f"  최종 hop weight (1홉/2홉) = {[round(x,3) for x in model._last_hopw.tolist()]}")
    if dev.type == "cuda":
        print(f"  peak GPU = {torch.cuda.max_memory_allocated()/1e9:.2f}GB / total 8.5GB")
    print(f"  vs exp27(sparse 1홉) 0.6906 / exp28(제품공간 dense A^2) 0.5708")


if __name__ == "__main__":
    try:
        main()
    except torch.cuda.OutOfMemoryError as e:
        print("\n" + "=" * 60)
        print("*** CUDA OOM — full-network dense A^2 crash test: 메모리 벽 확인 ***")
        print(f"peak allocated = {torch.cuda.max_memory_allocated()/1e9:.2f}GB / total 8.5GB")
        print(str(e).splitlines()[0])
    except Exception:
        traceback.print_exc()
