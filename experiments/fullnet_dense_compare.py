"""전체망(N×N) dense A² 공유-어텐션 Hadamard 모델 — 아키텍처 변형 비교.

확정 사항(사용자):
  2홉 후보를 'dense 무임계 전체망'(exp29 구조)으로 통일하고, exp29를 기준으로
  정규화·hidden·heads·hop구성·잔차 등을 변형해 best를 찾는다.

구조 (exp29 동일):
  X(N,d) = product content-agg / keyword·ip 학습 emb
  Z[h,i,j] = (q_i·k_j)/√d_k   (노드타입별 Q/K/V = HGT 격리)   (H,N,N)
  M = hop 결합(A, A², 정규화 적용)                            (N,N)
  attn = softmax_j(Z) ; E = attn ⊙ M ; E /= E.sum(j)
  out_i = Σ_j E[h,i,j] v_j ; emb = X(+잔차) → head(emb[:P])

변형 레버:
  norm    : A/A² 정규화 — 'max'(전역최대) | 'sym'(D^-1/2AD^-1/2) | 'row'(행정규화)
  hop_mode: 'sum'(w₁A+w₂A² 학습) | 'a_only'(1홉) | 'a2_only'(2홉)
  hidden, heads, dropout, residual

비교 기준점: exp10(전체망 스택) 0.678 / exp27(1홉, 퇴화 참고) 0.691
실행: python -m experiments.fullnet_dense_compare
"""
from __future__ import annotations

import math
import traceback

import torch
import torch.nn as nn
from sklearn.metrics import average_precision_score, roc_auc_score
from torch_geometric.utils import scatter

from src.data_builder.build_hetero_data import build_graph

LR, WD, EPOCHS, PATIENCE, POS_W, SEED = 0.005, 5e-4, 200, 30, 3.24, 42


# ─────────────────────────────────────────────────────────────────────
# 전체망 인접행렬 (N×N) + 순수 dense A² (임계컷 0) + 정규화
# ─────────────────────────────────────────────────────────────────────
def build_full_adjacency(data, P, K, I, norm="max", dev="cpu"):
    N = P + K + I
    A = torch.zeros(N, N, dtype=torch.float32)

    def add(ei, off_s, off_t):
        s, t = ei[0] + off_s, ei[1] + off_t
        A[s, t] = 1.0
        A[t, s] = 1.0

    add(data["product", "has_kw", "keyword"].edge_index, 0, P)
    add(data["product", "has_ip", "ip"].edge_index, 0, P + K)
    if ("ip", "has_kw", "keyword") in data.edge_types:
        add(data["ip", "has_kw", "keyword"].edge_index, P + K, P)
    if ("keyword", "trend_to", "keyword") in data.edge_types:
        add(data["keyword", "trend_to", "keyword"].edge_index, P, P)
    for et in [("product", "co_offline", "product"), ("product", "co_quick", "product")]:
        if et in data.edge_types:
            add(data[et].edge_index, 0, 0)
    A.fill_diagonal_(0.0)
    A2 = A @ A
    A2.fill_diagonal_(0.0)

    def _norm(M):
        if norm == "max":
            return M / (M.max() + 1e-9)
        if norm == "row":
            return M / (M.sum(-1, keepdim=True) + 1e-9)
        if norm == "sym":  # D^-1/2 M D^-1/2
            d = M.sum(-1).clamp(min=1e-9).pow(-0.5)
            return d.view(-1, 1) * M * d.view(1, -1)
        raise ValueError(norm)

    return _norm(A).to(dev), _norm(A2).to(dev), int((A > 0).sum()), int((A2 > 0).sum())


# ─────────────────────────────────────────────────────────────────────
class FullNetDense(nn.Module):
    def __init__(self, P, K, I, hidden=128, heads=4, dropout=0.3,
                 residual=True, hop_mode="sum"):
        super().__init__()
        self.P, self.K, self.I, self.N = P, K, I, P + K + I
        self.H, self.dk = heads, hidden // heads
        self.sqrt_dk = math.sqrt(self.dk)
        self.residual, self.hop_mode = residual, hop_mode
        self.keyword_emb = nn.Embedding(K, hidden)
        self.ip_emb = nn.Embedding(I, hidden)
        nn.init.xavier_uniform_(self.keyword_emb.weight)
        nn.init.xavier_uniform_(self.ip_emb.weight)
        self.product_feat_lin = nn.Linear(2, hidden)
        self.q_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.k_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.v_lin = nn.ModuleDict({t: nn.Linear(hidden, hidden) for t in ["product", "keyword", "ip"]})
        self.a_lin = nn.Linear(hidden, hidden)
        self.hop_logits = nn.Parameter(torch.zeros(2))
        self.drop = nn.Dropout(dropout)
        self.head = nn.Sequential(nn.Linear(hidden, hidden), nn.ReLU(),
                                  nn.Dropout(dropout), nn.Linear(hidden, 1))
        self._last_hopw = None

    def _init_X(self, pk_ei, pi_ei, has_promo, im30):
        dev = self.keyword_emb.weight.device
        d = self.keyword_emb.embedding_dim
        prod = torch.zeros(self.P, d, device=dev)
        prod = prod + scatter(self.keyword_emb(pk_ei[1]), pk_ei[0], dim=0, dim_size=self.P, reduce="mean")
        prod = prod + scatter(self.ip_emb(pi_ei[1]), pi_ei[0], dim=0, dim_size=self.P, reduce="mean")
        prod = prod + self.product_feat_lin(torch.stack([has_promo.float(), im30.float()], dim=-1))
        return torch.cat([prod, self.keyword_emb.weight, self.ip_emb.weight], dim=0)

    def _proj(self, lin, X):
        P, K = self.P, self.K
        out = torch.empty(self.N, self.H, self.dk, device=X.device)
        out[:P] = lin["product"](X[:P]).view(-1, self.H, self.dk)
        out[P:P + K] = lin["keyword"](X[P:P + K]).view(-1, self.H, self.dk)
        out[P + K:] = lin["ip"](X[P + K:]).view(-1, self.H, self.dk)
        return out

    def forward(self, pk_ei, pi_ei, A, A2, has_promo, im30):
        X = self._init_X(pk_ei, pi_ei, has_promo, im30)
        q, k, v = self._proj(self.q_lin, X), self._proj(self.k_lin, X), self._proj(self.v_lin, X)
        Z = torch.einsum("nhd,mhd->hnm", q, k) / self.sqrt_dk
        if self.hop_mode == "a_only":
            M = A
        elif self.hop_mode == "a2_only":
            M = A2
        else:
            hopw = torch.softmax(self.hop_logits, dim=0)
            self._last_hopw = hopw.detach().cpu()
            M = hopw[0] * A + hopw[1] * A2
        attn = torch.softmax(Z, dim=-1)
        E = attn * M.unsqueeze(0)
        E = E / (E.sum(dim=-1, keepdim=True) + 1e-9)
        E = self.drop(E)
        out = torch.einsum("hnm,mhd->nhd", E, v).reshape(self.N, -1)
        emb = (X + self.a_lin(out)) if self.residual else self.a_lin(out)
        return self.head(emb[:self.P]).squeeze(-1)


# ─────────────────────────────────────────────────────────────────────
def run_variant(name, data, P, K, I, dev, *, norm="max", hop_mode="sum",
                hidden=128, heads=4, dropout=0.3, residual=True):
    torch.manual_seed(SEED)
    if dev.type == "cuda":
        torch.cuda.empty_cache(); torch.cuda.reset_peak_memory_stats()
    pk = data["product", "has_kw", "keyword"].edge_index.to(dev)
    pi = data["product", "has_ip", "ip"].edge_index.to(dev)
    y = data["product"].y.to(dev)
    hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    tr = data["product"].train_mask.to(dev)
    va = data["product"].val_mask.to(dev)
    te = data["product"].test_mask.to(dev)

    A, A2, _, _ = build_full_adjacency(data, P, K, I, norm=norm, dev=dev)
    model = FullNetDense(P, K, I, hidden, heads, dropout, residual, hop_mode).to(dev)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([POS_W], device=dev))

    def pr(mask):
        model.eval()
        with torch.no_grad():
            lg = model(pk, pi, A, A2, hp, im30)
            p = torch.sigmoid(lg[mask]).cpu().numpy(); t = y[mask].cpu().numpy()
        return average_precision_score(t, p), roc_auc_score(t, p)

    best, best_state, wait = -1.0, None, 0
    for ep in range(1, EPOCHS + 1):
        model.train(); opt.zero_grad()
        loss = crit(model(pk, pi, A, A2, hp, im30)[tr], y[tr])
        loss.backward(); opt.step()
        vpr, _ = pr(va)
        if vpr > best:
            best, wait = vpr, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            wait += 1
        if wait >= PATIENCE:
            break
    model.load_state_dict(best_state)
    tpr, tauc = pr(te)
    peak = torch.cuda.max_memory_allocated() / 1e9 if dev.type == "cuda" else 0
    hopw = [round(x, 3) for x in model._last_hopw.tolist()] if model._last_hopw is not None else None
    del model, A, A2
    if dev.type == "cuda":
        torch.cuda.empty_cache()
    return {"name": name, "norm": norm, "hop": hop_mode, "hid": hidden, "heads": heads,
            "residual": residual, "test_pr": round(tpr, 4), "test_auc": round(tauc, 4),
            "val_pr": round(best, 4), "epochs": ep, "hopw": hopw, "peakGB": round(peak, 2)}


# ─────────────────────────────────────────────────────────────────────
# exp28 — 제품공간(P×P) dense A² (참고: 직접 P-K 어텐션 누락 → 성능 낮음)
# ─────────────────────────────────────────────────────────────────────
class ProductSpaceDense(nn.Module):
    def __init__(self, n_kw, n_ip, hidden=128, heads=4, dropout=0.3):
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
        self.hop_logits = nn.Parameter(torch.zeros(2))
        self.drop = nn.Dropout(dropout)
        self.head = nn.Sequential(nn.Linear(hidden, hidden), nn.ReLU(),
                                  nn.Dropout(dropout), nn.Linear(hidden, 1))
        self._last_hopw = None

    def _init_product(self, pk, pi, P, hp, im30):
        dev = self.keyword_emb.weight.device
        acc = torch.zeros(P, self.keyword_emb.embedding_dim, device=dev)
        acc = acc + scatter(self.keyword_emb(pk[1]), pk[0], dim=0, dim_size=P, reduce="mean")
        acc = acc + scatter(self.ip_emb(pi[1]), pi[0], dim=0, dim_size=P, reduce="mean")
        return acc + self.product_feat_lin(torch.stack([hp.float(), im30.float()], dim=-1))

    def forward(self, pk, pi, A1, A2, hp, im30):
        P, H, dk = A1.size(0), self.H, self.dk
        Hp = self._init_product(pk, pi, P, hp, im30)
        q = self.q_lin(Hp).view(P, H, dk); k = self.k_lin(Hp).view(P, H, dk); v = self.v_lin(Hp).view(P, H, dk)
        Z = torch.einsum("phd,qhd->hpq", q, k) / self.sqrt_dk
        hopw = torch.softmax(self.hop_logits, dim=0); self._last_hopw = hopw.detach().cpu()
        M = hopw[0] * A1 + hopw[1] * A2
        attn = torch.softmax(Z, dim=-1)
        E = attn * M.unsqueeze(0); E = E / (E.sum(dim=-1, keepdim=True) + 1e-9); E = self.drop(E)
        out = torch.einsum("hpq,qhd->phd", E, v).reshape(P, H * dk)
        return self.head(Hp + self.a_lin(out)).squeeze(-1)


def build_product_adjacency(data, P, K, I, dev="cpu"):
    """제품공간 A1(co-purchase P×P) + A2(P-K-P+P-I-P, dense, 임계컷 0)."""
    def _adj(ei, n):
        A = torch.zeros(n, n, dtype=torch.float32)
        A[ei[0], ei[1]] = 1.0; A[ei[1], ei[0]] = 1.0; A.fill_diagonal_(0.0)
        return A

    def _hop2(ei, n_dst):
        B = torch.zeros(P, n_dst, dtype=torch.float32); B[ei[0], ei[1]] = 1.0
        A2 = B @ B.t(); A2.fill_diagonal_(0.0); return A2

    co = [data[et].edge_index for et in
          [("product", "co_offline", "product"), ("product", "co_quick", "product")] if et in data.edge_types]
    co_ei = torch.cat(co, dim=1) if co else torch.empty(2, 0, dtype=torch.long)
    A1 = _adj(co_ei, P)
    A2 = _hop2(data["product", "has_kw", "keyword"].edge_index, K) + \
        _hop2(data["product", "has_ip", "ip"].edge_index, I)
    A1 = A1 / (A1.max() + 1e-9); A2 = A2 / (A2.max() + 1e-9)
    return A1.to(dev), A2.to(dev)


def run_product_space(data, P, K, I, dev, hidden=128, heads=4, dropout=0.3):
    """exp28 — 제품공간 dense A² 학습 → 지표 dict."""
    torch.manual_seed(SEED)
    if dev.type == "cuda":
        torch.cuda.empty_cache(); torch.cuda.reset_peak_memory_stats()
    pk = data["product", "has_kw", "keyword"].edge_index.to(dev)
    pi = data["product", "has_ip", "ip"].edge_index.to(dev)
    y = data["product"].y.to(dev); hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    tr, va, te = (data["product"].train_mask.to(dev), data["product"].val_mask.to(dev), data["product"].test_mask.to(dev))
    A1, A2 = build_product_adjacency(data, P, K, I, dev)
    model = ProductSpaceDense(K, I, hidden, heads, dropout).to(dev)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([POS_W], device=dev))

    def pr(mask):
        model.eval()
        with torch.no_grad():
            p = torch.sigmoid(model(pk, pi, A1, A2, hp, im30)[mask]).cpu().numpy(); t = y[mask].cpu().numpy()
        return average_precision_score(t, p), roc_auc_score(t, p)

    best, best_state, wait = -1.0, None, 0
    for ep in range(1, EPOCHS + 1):
        model.train(); opt.zero_grad()
        crit(model(pk, pi, A1, A2, hp, im30)[tr], y[tr]).backward(); opt.step()
        vpr, _ = pr(va)
        if vpr > best:
            best, wait = vpr, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            wait += 1
        if wait >= PATIENCE:
            break
    model.load_state_dict(best_state); tpr, tauc = pr(te)
    peak = torch.cuda.max_memory_allocated() / 1e9 if dev.type == "cuda" else 0
    return {"name": "exp28_제품공간dense", "test_pr": round(tpr, 4), "test_auc": round(tauc, 4),
            "val_pr": round(best, 4), "hopw": [round(x, 3) for x in model._last_hopw.tolist()],
            "peakGB": round(peak, 2)}


# ─────────────────────────────────────────────────────────────────────
# exp30 — Semantic Fusion: 정제 3중 행렬, K-K·I-K 제외 (노이즈 수프 필터)
#   M = g₁·Â_1hop_all(P-P·P-K·P-I) + g₂·Â_sim_ip(P-I-P≥1) + g₃·Â_sim_kw(P-K-P≥5)
#   전체 dense 2홉(공선성·노이즈)은 제외 — sim이 곧 2홉의 유의미 부분집합.
# ─────────────────────────────────────────────────────────────────────
def _sym(M):
    d = M.sum(-1).clamp(min=1e-9).pow(-0.5)
    return d.view(-1, 1) * M * d.view(1, -1)


def build_semantic_matrices(data, P, K, I, sim_kw_thr=5, sim_ip_thr=1, dev="cpu"):
    """3중 행렬 + nnz. K-K(trend_to)·I-K(ip-has_kw) 제외. 각 sym 정규화."""
    N = P + K + I
    A1 = torch.zeros(N, N, dtype=torch.float32)

    def add(ei, os_, ot):
        A1[ei[0] + os_, ei[1] + ot] = 1.0
        A1[ei[1] + ot, ei[0] + os_] = 1.0

    add(data["product", "has_kw", "keyword"].edge_index, 0, P)      # P-K
    add(data["product", "has_ip", "ip"].edge_index, 0, P + K)       # P-I
    for et in [("product", "co_offline", "product"), ("product", "co_quick", "product")]:
        if et in data.edge_types:
            add(data[et].edge_index, 0, 0)                          # P-P (영수증)
    A1.fill_diagonal_(0.0)                                          # ← K-K·I-K 미포함

    def sim_block(ei, n_dst, thr):
        B = torch.zeros(P, n_dst, dtype=torch.float32)
        B[ei[0], ei[1]] = 1.0
        S = (B @ B.t() >= thr).float()
        S.fill_diagonal_(0.0)
        M = torch.zeros(N, N, dtype=torch.float32)
        M[:P, :P] = S
        return M

    A_sim_ip = sim_block(data["product", "has_ip", "ip"].edge_index, I, sim_ip_thr)
    A_sim_kw = sim_block(data["product", "has_kw", "keyword"].edge_index, K, sim_kw_thr)
    nnz = {"1hop": int((A1 > 0).sum()), "sim_ip": int((A_sim_ip > 0).sum()),
           "sim_kw": int((A_sim_kw > 0).sum())}
    mats = [_sym(A1).to(dev), _sym(A_sim_ip).to(dev), _sym(A_sim_kw).to(dev)]
    return mats, nnz


class SemanticFusionDense(FullNetDense):
    """3중(또는 n중) 행렬 DiffMG 가중합 → 공유 어텐션 ⊙ M → 1층 readout."""
    def __init__(self, P, K, I, n_mats=3, hidden=128, heads=4, dropout=0.3):
        super().__init__(P, K, I, hidden, heads, dropout, residual=True, hop_mode="sum")
        self.hop_logits = nn.Parameter(torch.zeros(n_mats))

    def forward(self, pk, pi, mats, hp, im30):
        X = self._init_X(pk, pi, hp, im30)
        q, k, v = self._proj(self.q_lin, X), self._proj(self.k_lin, X), self._proj(self.v_lin, X)
        Z = torch.einsum("nhd,mhd->hnm", q, k) / self.sqrt_dk
        g = torch.softmax(self.hop_logits, dim=0)
        self._last_hopw = g.detach().cpu()
        M = sum(g[i] * mats[i] for i in range(len(mats)))
        attn = torch.softmax(Z, dim=-1)
        E = attn * M.unsqueeze(0)
        E = E / (E.sum(dim=-1, keepdim=True) + 1e-9)
        E = self.drop(E)
        out = torch.einsum("hnm,mhd->nhd", E, v).reshape(self.N, -1)
        emb = X + self.a_lin(out)
        return self.head(emb[:self.P]).squeeze(-1)


def run_semantic_fusion(data, P, K, I, dev, sim_kw_thr=5, sim_ip_thr=1,
                        hidden=128, heads=4, dropout=0.3):
    """exp30 — Semantic Fusion (정제 3중 행렬) 학습 → 지표 dict."""
    torch.manual_seed(SEED)
    if dev.type == "cuda":
        torch.cuda.empty_cache(); torch.cuda.reset_peak_memory_stats()
    pk = data["product", "has_kw", "keyword"].edge_index.to(dev)
    pi = data["product", "has_ip", "ip"].edge_index.to(dev)
    y = data["product"].y.to(dev); hp = data["product"].has_promo.to(dev)
    im30 = data["product"].insta_mention_30d.to(dev)
    tr, va, te = (data["product"].train_mask.to(dev), data["product"].val_mask.to(dev), data["product"].test_mask.to(dev))
    mats, nnz = build_semantic_matrices(data, P, K, I, sim_kw_thr, sim_ip_thr, dev)
    model = SemanticFusionDense(P, K, I, len(mats), hidden, heads, dropout).to(dev)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    crit = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([POS_W], device=dev))

    def pr(mask):
        model.eval()
        with torch.no_grad():
            p = torch.sigmoid(model(pk, pi, mats, hp, im30)[mask]).cpu().numpy(); t = y[mask].cpu().numpy()
        return average_precision_score(t, p), roc_auc_score(t, p)

    best, best_state, wait = -1.0, None, 0
    for ep in range(1, EPOCHS + 1):
        model.train(); opt.zero_grad()
        crit(model(pk, pi, mats, hp, im30)[tr], y[tr]).backward(); opt.step()
        vpr, _ = pr(va)
        if vpr > best:
            best, wait = vpr, 0
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
        else:
            wait += 1
        if wait >= PATIENCE:
            break
    model.load_state_dict(best_state); tpr, tauc = pr(te)
    peak = torch.cuda.max_memory_allocated() / 1e9 if dev.type == "cuda" else 0
    return {"name": "exp30_semantic_fusion", "test_pr": round(tpr, 4), "test_auc": round(tauc, 4),
            "val_pr": round(best, 4),
            "gate(1hop/sim_ip/sim_kw)": [round(x, 3) for x in model._last_hopw.tolist()],
            "nnz": nnz, "peakGB": round(peak, 2)}


# 비교할 변형 목록 (전부 dense 무임계 전체망)
VARIANTS = [
    dict(name="base(exp29)",      norm="max", hop_mode="sum"),
    dict(name="sym_norm",         norm="sym", hop_mode="sum"),
    dict(name="row_norm",         norm="row", hop_mode="sum"),
    dict(name="sym+1hop",         norm="sym", hop_mode="a_only"),
    dict(name="sym+2hop",         norm="sym", hop_mode="a2_only"),
    dict(name="sym+h64",          norm="sym", hop_mode="sum", hidden=64),
    dict(name="sym+no_resid",     norm="sym", hop_mode="sum", residual=False),
    dict(name="row+h64",          norm="row", hop_mode="sum", hidden=64),
]


def main():
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"device={dev}  {torch.cuda.get_device_name(0) if dev.type=='cuda' else ''}")
    data, _ = build_graph(seed=SEED, include_offline_copurchase=True,
                          include_quick_copurchase=True, use_lift_weights=False, add_2hop_edges=False)
    P = data["product"].num_nodes
    K, I = data["keyword"].num_nodes, data["ip"].num_nodes
    print(f"N={P+K+I} (P={P} K={K} I={I})\n")

    rows = []
    for cfg in VARIANTS:
        try:
            r = run_variant(data=data, P=P, K=K, I=I, dev=dev, **cfg)
            rows.append(r)
            print(f"  {r['name']:16s} norm={r['norm']:3s} hop={r['hop']:7s} h={r['hid']} "
                  f"| test_pr={r['test_pr']} auc={r['test_auc']} hopw={r['hopw']} peak={r['peakGB']}GB")
        except torch.cuda.OutOfMemoryError:
            print(f"  {cfg['name']:16s} → CUDA OOM (skip)")
            torch.cuda.empty_cache()
        except Exception:
            traceback.print_exc()

    print("\n" + "=" * 78)
    print("순위 (test PR-AUC):  [참고] exp10 전체망스택 0.678 / exp27 1홉 0.691")
    for r in sorted(rows, key=lambda x: -x["test_pr"]):
        print(f"  {r['test_pr']:.4f}  {r['name']:16s} (norm={r['norm']}, hop={r['hop']}, h={r['hid']}, resid={r['residual']})")


if __name__ == "__main__":
    main()
