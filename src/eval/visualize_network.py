"""시드 키워드 에고 네트워크 인터랙티브 시각화 (PyVis).

학습된 가중치만 사용. 메타패스 keyword_s → product → keyword_t 를
3층 그래프로 그려, "시드 키워드가 어떤 엣지를 타고 조합을 이루는지" 와
"그 조합으로 만들어진 제품 예시" 를 한 장에 보여준다.

  · 가운데  : 시드 키워드 (별 모양)
  · 1-hop   : 시드를 가진 제품(=조합 예시). 색/크기 = 예측 성공확률
  · 2-hop   : 제품의 다른 키워드(=추천 조합). 점수 = 추천 메타패스 점수
  · 엣지 두께: 학습된 (product, has_kw, keyword) 어텐션

실행:
  python -m src.eval.visualize_network --seed 마라
  python -m src.eval.visualize_network --seed 위스키 --top-prod 10 --top-kw 15
"""
import argparse
import math
import os
from collections import defaultdict

import torch

from src.data_builder.build_hetero_data import build_graph, forward_edge_index_dict
from src.models.hin_gnn import HINGNN
from src.eval.recommend import _build_artifacts

CKPT = "checkpoints/hin_gnn_best.pt"
OUT_DIR = "docs/network_viz"


def _rebuild(cfg, data, dev):
    m = cfg["model"]
    model = HINGNN(
        cfg["graph"]["node_types"], [tuple(e) for e in cfg["graph"]["edge_types"]],
        {"product": data["product"].num_nodes, "keyword": data["keyword"].num_nodes,
         "ip": data["ip"].num_nodes},
        m["hidden_dim"], m["num_layers"], m["num_heads"], m["dropout"],
        m["use_diffmg_gate"], m["diffmg_temperature"],
        cfg["node_feat"]["product_aggr"], cfg["node_feat"]["use_has_promo_feature"],
    ).to(dev)
    return model


def _prob_color(p: float) -> str:
    """성공확률 0→1 을 연한 회색→진한 빨강 으로."""
    p = max(0.0, min(1.0, p))
    r = int(210 - 40 * p)
    g = int(210 - 170 * p)
    b = int(210 - 170 * p)
    return f"rgb({r},{g},{b})"


def _load():
    """체크포인트 로드 → forward 1회 → (성공확률 p, prod2kw, kw2prod, maps) 캐시."""
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    ckpt = torch.load(CKPT, weights_only=False)
    cfg, maps = ckpt["config"], ckpt["maps"]

    data, _ = build_graph(seed=cfg["split"]["seed"], ratios=tuple(cfg["split"]["ratios"]))
    eidx = {et: ei.to(dev) for et, ei in forward_edge_index_dict(data).items()}
    hp = data["product"].has_promo.to(dev)
    model = _rebuild(cfg, data, dev)
    model.load_state_dict(ckpt["model"]); model.eval()

    p, prod2kw, kw2prod = _build_artifacts(model, eidx, hp)
    return p, prod2kw, kw2prod, maps


def build_chain(seed: str, steps: int):
    """그리디 best-first 사슬: 시드에서 매 홉 최대 어텐션 엣지를 따라
    keyword → product → keyword → product … 한 줄로 순회.

    · keyword→product : 해당 키워드 att 최대 제품 (동률이면 성공확률로 tie-break)
    · product→keyword : 그 제품의 (방문 안 한) 키워드 중 att 최대
    노드는 재방문하지 않아 사슬이 꼬이지 않음.
    """
    p, prod2kw, kw2prod, maps = _load()
    k2i, kw_ids, pn = maps["k2i"], maps["keyword_ids"], maps["product_names"]
    if seed not in k2i:
        raise KeyError(f"시드 키워드가 그래프에 없음: '{seed}' (정규화로 다른 형태일 수 있음)")

    cur = k2i[seed]
    visited_kw = {cur}
    visited_prod = set()
    # path: [("kw", idx, in_att), ...] in_att = 직전 엣지 어텐션(시드는 None)
    path = [("kw", cur, None)]
    for _ in range(steps):
        pc = [(j, a) for j, a in kw2prod.get(cur, []) if j not in visited_prod]
        if not pc:
            break
        j_star, att_in = max(pc, key=lambda x: (round(x[1], 6), float(p[x[0]])))
        visited_prod.add(j_star)
        kc = [(kt, a) for kt, a in prod2kw.get(j_star, []) if kt not in visited_kw]
        if not kc:
            path.append(("prod", j_star, att_in))
            break
        kt_star, att_out = max(kc, key=lambda x: x[1])
        visited_kw.add(kt_star)
        path.append(("prod", j_star, att_in))
        path.append(("kw", kt_star, att_out))
        cur = kt_star

    return dict(seed=seed, path=path, p=p, pn=pn, kw_ids=kw_ids)


def render_chain(g, out_path: str):
    from pyvis.network import Network

    net = Network(height="600px", width="100%", directed=True,
                  bgcolor="#ffffff", font_color="#222222",
                  cdn_resources="in_line", notebook=False)

    path, p, pn, kw_ids = g["path"], g["p"], g["pn"], g["kw_ids"]
    atts = [a for _, _, a in path if a is not None]
    amax = max(atts) if atts else 1.0

    def w(a):
        return 1.0 + 11.0 * (a / amax)

    prev = None
    for i, node in enumerate(path):
        x = i * 240 - (len(path) - 1) * 120
        if node[0] == "kw":
            _, idx, in_att = node
            nid = f"k_{idx}"
            if i == 0:
                net.add_node(nid, label=kw_ids[idx], shape="star", color="#1f77b4",
                             size=40, x=x, y=0, physics=False, fixed=True,
                             title=f"시드: {kw_ids[idx]}", font={"size": 24})
            else:
                net.add_node(nid, label=kw_ids[idx], shape="dot", color="#2ca02c",
                             size=24, x=x, y=0, physics=False, fixed=True,
                             title=f"키워드: {kw_ids[idx]}", font={"size": 20})
        else:
            _, idx, in_att = node
            nid = f"p_{idx}"
            pj = float(p[idx])
            net.add_node(nid, label=pn[idx], shape="dot", color=_prob_color(pj),
                         size=16 + 24 * pj, x=x, y=130, physics=False, fixed=True,
                         title=f"{pn[idx]}<br>예측 성공확률: {pj:.3f}",
                         font={"size": 16})
        if prev is not None:
            in_att = node[2]
            net.add_edge(prev, nid, value=w(in_att), color="#777777",
                         label=f"{in_att:.3f}", font={"size": 14, "align": "top"},
                         title=f"att={in_att:.4f}")
        prev = nid

    net.set_options('{"physics":{"enabled":false},'
                    '"interaction":{"hover":true,"tooltipDelay":80},'
                    '"edges":{"smooth":{"type":"cubicBezier"}}}')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    net.write_html(out_path, notebook=False)
    return out_path


def build_combo(seed: str, top_kw: int, ex_per_kw: int,
                rel_cutoff: float = 0.15, kw_per_prod: int = 0,
                leaf_rel: float = 0.3):
    """combo-ego: 시드와 '유의미하게' 붙는 키워드 조합 + 그 조합의 예시 제품.

    조합 점수(빈도 보정):
        raw(kt)  = Σ_{j∈마라제품}  att(j, 시드) · p(j) · att(j, kt)
        score(kt)= raw(kt) / (1 + log(df(kt)))     # df=kt 보유 제품 수
      → att(j,kt) 가 키워드 기준 정규화라 희귀 키워드가 과대평가되는 편향을 교정.
    상대 컷: score < rel_cutoff · max(score) 인 조합 제거 (long-tail 노이즈 제거).
    예시 제품: 시드·kt 를 둘 다 가진 제품을 att(j,kt)·p(j) 순으로 ex_per_kw 개.
    """
    p, prod2kw, kw2prod, maps = _load()
    k2i, kw_ids, pn = maps["k2i"], maps["keyword_ids"], maps["product_names"]
    if seed not in k2i:
        raise KeyError(f"시드 키워드가 그래프에 없음: '{seed}' (정규화로 다른 형태일 수 있음)")
    ks = k2i[seed]

    # 시드를 가진 제품 → att_in
    seed_prod = {j: att_in for j, att_in in kw2prod.get(ks, [])}

    raw = defaultdict(float)
    cooccur = defaultdict(list)        # kt -> [(j, att_out)]  (시드+kt 둘 다 가진 제품)
    for j, att_in in seed_prod.items():
        pj = float(p[j])
        for kt, att_out in prod2kw.get(j, []):
            if kt == ks:
                continue
            raw[kt] += att_in * pj * att_out
            cooccur[kt].append((j, att_out))

    # 빈도 보정
    score = {kt: r / (1.0 + math.log(len(kw2prod.get(kt, [1]))))
             for kt, r in raw.items()}
    ranked = sorted(score.items(), key=lambda x: x[1], reverse=True)
    if not ranked:
        raise ValueError(f"'{seed}' 와 공동출현하는 키워드가 없음")
    top_s = ranked[0][1]
    ranked = [(kt, s) for kt, s in ranked if s >= rel_cutoff * top_s][:top_kw]

    combos = []
    for kt, s in ranked:
        ex = sorted(cooccur[kt], key=lambda x: x[1] * float(p[x[0]]), reverse=True)
        ex = ex[:ex_per_kw]
        combos.append(dict(kt=kt, score=s, df=len(kw2prod.get(kt, [])),
                           examples=[(j, a, float(p[j])) for j, a in ex]))

    # L3: 예시 제품의 '유효 키워드' (제품 내 att >= leaf_rel·max, 시드·L1조합 제외)
    leaves = {}
    if kw_per_prod > 0:
        combo_set = {c["kt"] for c in combos}
        uniq_prod = {j for c in combos for (j, _, _) in c["examples"]}
        for j in uniq_prod:
            kws = prod2kw.get(j, [])
            if not kws:
                continue
            mx = max(a for _, a in kws)
            cand = [(kt, a) for kt, a in kws
                    if kt != ks and kt not in combo_set and a >= leaf_rel * mx]
            cand.sort(key=lambda x: x[1], reverse=True)
            if cand[:kw_per_prod]:
                leaves[j] = cand[:kw_per_prod]

    return dict(seed=seed, ks=ks, combos=combos, leaves=leaves,
                p=p, pn=pn, kw_ids=kw_ids)


def render_combo(g, out_path: str):
    from pyvis.network import Network

    net = Network(height="820px", width="100%", directed=True,
                  bgcolor="#ffffff", font_color="#222222",
                  cdn_resources="in_line", notebook=False)

    pn, kw_ids, p = g["pn"], g["kw_ids"], g["p"]
    smax = max(c["score"] for c in g["combos"]) or 1.0
    amax = max((e[1] for c in g["combos"] for e in c["examples"]), default=1.0) or 1.0

    # 시드 (level 0)
    net.add_node(f"k_{g['ks']}", label=g["seed"], shape="star", color="#1f77b4",
                 size=42, level=0, title=f"시드: {g['seed']}", font={"size": 26})

    seen_prod = set()
    for c in g["combos"]:
        kid = f"k_{c['kt']}"
        net.add_node(kid, label=kw_ids[c["kt"]], shape="dot", color="#2ca02c",
                     size=20 + 22 * (c["score"] / smax), level=1,
                     title=f"{kw_ids[c['kt']]}<br>조합점수(보정): {c['score']:.5f}"
                           f"<br>보유 제품수 df: {c['df']}",
                     font={"size": 20})
        net.add_edge(f"k_{g['ks']}", kid, value=1 + 10 * (c["score"] / smax),
                     color="#9ecae1", title=f"조합점수 {c['score']:.5f}")
        for j, att_out, pj in c["examples"]:
            pid = f"p_{j}"
            if pid not in seen_prod:
                net.add_node(pid, label=pn[j], shape="dot", color=_prob_color(pj),
                             size=14 + 24 * pj, level=2,
                             title=f"{pn[j]}<br>예측 성공확률: {pj:.3f}",
                             font={"size": 15})
                seen_prod.add(pid)
            net.add_edge(kid, pid, value=1 + 9 * (att_out / amax),
                         color="#c7e9c0", title=f"att={att_out:.4f}")

    # L3: 예시 제품의 유효 키워드 (orange)
    leaves = g.get("leaves") or {}
    if leaves:
        lmax = max((a for lst in leaves.values() for _, a in lst), default=1.0) or 1.0
        seen_leaf = set()
        for j, lst in leaves.items():
            pid = f"p_{j}"
            for kt, a in lst:
                lid = f"k_{kt}"
                if lid not in seen_leaf:
                    net.add_node(lid, label=kw_ids[kt], shape="dot", color="#fdae6b",
                                 size=13, level=3,
                                 title=f"{kw_ids[kt]} (제품 유효 키워드)<br>att={a:.4f}",
                                 font={"size": 14})
                    seen_leaf.add(lid)
                net.add_edge(pid, lid, value=1 + 8 * (a / lmax),
                             color="#fdd0a2", title=f"att={a:.4f}")

    net.set_options(
        '{"layout":{"hierarchical":{"enabled":true,"direction":"LR",'
        '"sortMethod":"directed","levelSeparation":260,"nodeSpacing":110,'
        '"treeSpacing":140}},"physics":{"enabled":false},'
        '"interaction":{"hover":true,"tooltipDelay":80},'
        '"edges":{"smooth":{"type":"cubicBezier","forceDirection":"horizontal"}}}'
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    net.write_html(out_path, notebook=False)
    return out_path


def build_ego(seed: str, top_prod: int, top_kw: int):
    p, prod2kw, kw2prod, maps = _load()
    k2i, kw_ids, pn = maps["k2i"], maps["keyword_ids"], maps["product_names"]
    if seed not in k2i:
        raise KeyError(f"시드 키워드가 그래프에 없음: '{seed}' (정규화로 다른 형태일 수 있음)")
    ks = k2i[seed]

    # 1) 시드를 가진 제품 → att_in × 성공확률 로 랭킹 → top_prod
    prods = [(j, att_in, float(p[j])) for j, att_in in kw2prod.get(ks, [])]
    prods.sort(key=lambda x: x[1] * x[2], reverse=True)
    prods = prods[:top_prod]
    sel_prod = {j for j, _, _ in prods}

    # 2) 선택 제품들의 조합 키워드 → 메타패스 점수 합산 → top_kw
    kw_score = defaultdict(float)
    kw_edges = defaultdict(list)   # kt -> [(j, att_out)]
    for j, att_in, pj in prods:
        for kt, att_out in prod2kw.get(j, []):
            if kt == ks:
                continue
            kw_score[kt] += att_in * pj * att_out
            kw_edges[kt].append((j, att_out))
    top_kt = sorted(kw_score.items(), key=lambda x: x[1], reverse=True)[:top_kw]
    sel_kw = {kt for kt, _ in top_kt}

    return dict(seed=seed, ks=ks, prods=prods, top_kt=top_kt, kw_edges=kw_edges,
                sel_prod=sel_prod, sel_kw=sel_kw, p=p, pn=pn, kw_ids=kw_ids)


def render(g, out_path: str):
    from pyvis.network import Network

    net = Network(height="800px", width="100%", directed=True,
                  bgcolor="#ffffff", font_color="#222222",
                  cdn_resources="in_line", notebook=False)
    net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=120)

    seed, ks = g["seed"], g["ks"]
    pn, kw_ids, p = g["pn"], g["kw_ids"], g["p"]

    # 엣지 두께 스케일용 정규화
    atts = [a for _, a, _ in g["prods"]] + \
           [ao for kt, _ in g["top_kt"] for _, ao in g["kw_edges"][kt]]
    amax = max(atts) if atts else 1.0

    def w(a):
        return 1.0 + 9.0 * (a / amax)

    # 시드 노드
    net.add_node(f"k_{ks}", label=seed, shape="star", color="#1f77b4",
                 size=40, title=f"시드 키워드: {seed}", font={"size": 26})

    # 제품 노드 (조합 예시) — 시드→제품 엣지는 구조 엣지(etype=seed), 항상 표시
    for j, att_in, pj in g["prods"]:
        net.add_node(f"p_{j}", label=pn[j], shape="dot", size=14 + 26 * pj,
                     color=_prob_color(pj),
                     title=f"{pn[j]}<br>예측 성공확률: {pj:.3f}<br>시드 어텐션: {att_in:.4f}",
                     font={"size": 16})
        net.add_edge(f"k_{ks}", f"p_{j}", value=w(att_in), color="#9ecae1",
                     title=f"att={att_in:.4f}", att=round(att_in, 6), etype="seed")

    # 조합 키워드 노드 + 제품→키워드(조합) 엣지 — etype=combo, 슬라이더 필터 대상
    for kt, sc in g["top_kt"]:
        net.add_node(f"k_{kt}", label=kw_ids[kt], shape="dot", size=18,
                     color="#2ca02c",
                     title=f"{kw_ids[kt]}<br>추천 조합 점수: {sc:.5f}",
                     font={"size": 18})
        for j, att_out in g["kw_edges"][kt]:
            if j in g["sel_prod"]:
                net.add_edge(f"p_{j}", f"k_{kt}", value=w(att_out),
                             color="#c7e9c0", title=f"att={att_out:.4f}",
                             att=round(att_out, 6), etype="combo")

    net.set_options('{"physics":{"stabilization":{"iterations":300}},'
                    '"interaction":{"hover":true,"tooltipDelay":80}}')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    net.write_html(out_path, notebook=False)
    _inject_slider(out_path, f"k_{ks}", seed)
    return out_path


def _inject_slider(html_path: str, seed_id: str, seed_label: str):
    """생성된 PyVis HTML 에 조합 엣지 어텐션 임계값 슬라이더 주입."""
    with open(html_path, encoding="utf-8") as f:
        html = f.read()

    control = (
        '<div id="ctrl" style="position:fixed;top:12px;left:12px;z-index:999;'
        'background:#fff;border:1px solid #ccc;border-radius:8px;padding:10px 14px;'
        'font-family:sans-serif;box-shadow:0 2px 8px rgba(0,0,0,.18);">'
        f'<b>조합 엣지 어텐션 임계값</b> &nbsp;(시드: {seed_label})<br>'
        '<input type="range" id="attThresh" min="0" max="1" step="0.01" value="0" '
        'style="width:240px;vertical-align:middle;">'
        '<span id="attVal" style="font-weight:bold;">0.000</span><br>'
        '<small id="attCount" style="color:#666;"></small><br>'
        '<small style="color:#888;">제품→키워드(조합) 엣지만 필터. '
        '시드→제품 구조 엣지는 항상 표시.</small></div>'
    )

    script = (
        '<script type="text/javascript">(function(){'
        f'var SEED_ID="{seed_id}";'
        'function init(){'
        'if(typeof edges==="undefined"||typeof network==="undefined"||!edges){'
        'setTimeout(init,120);return;}'
        'var allE=edges.get(),allN=nodes.get();'
        'var s=document.getElementById("attThresh"),'
        'lab=document.getElementById("attVal"),cnt=document.getElementById("attCount");'
        'function apply(){'
        'var th=parseFloat(s.value);lab.textContent=th.toFixed(3);'
        'var kwOn={},shown=0,total=0,eu=[];'
        'allE.forEach(function(e){var v=true;'
        'if(e.etype==="combo"){total++;v=(e.att>=th);if(v){shown++;kwOn[e.to]=true;}}'
        'eu.push({id:e.id,hidden:!v});});'
        'edges.update(eu);'
        'var nu=[];allN.forEach(function(n){'
        'if(String(n.id).indexOf("k_")===0&&n.id!==SEED_ID){'
        'nu.push({id:n.id,hidden:!kwOn[n.id]});}});'
        'nodes.update(nu);'
        'cnt.textContent="표시 조합 엣지 "+shown+" / "+total;}'
        's.addEventListener("input",apply);apply();}'
        'init();})();</script>'
    )

    html = html.replace("</body>", control + script + "</body>", 1)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["combo", "combo3", "chain", "ego"],
                    default="combo",
                    help="combo=조합키워드+예시제품 / combo3=+제품 유효키워드(4단) / "
                         "chain=최대엣지 순회 / ego=에고")
    ap.add_argument("--seed", default="마라", help="시드 키워드")
    ap.add_argument("--steps", type=int, default=6, help="[chain] keyword 홉 수")
    ap.add_argument("--top-prod", type=int, default=8, help="[ego] 노출 제품 수")
    ap.add_argument("--top-kw", type=int, default=8, help="[combo/ego] 조합 키워드 수")
    ap.add_argument("--ex-per-kw", type=int, default=3, help="[combo] 조합당 예시 제품 수")
    ap.add_argument("--rel-cutoff", type=float, default=0.15,
                    help="[combo] 최고점 대비 이 비율 미만 조합 제거")
    ap.add_argument("--kw-per-prod", type=int, default=5,
                    help="[combo3] 제품당 노출할 유효 키워드 수 (L3)")
    ap.add_argument("--leaf-rel", type=float, default=0.1,
                    help="[combo3] 제품 내 max att 대비 이 비율 이상만 유효키워드로")
    ap.add_argument("--out", default=None, help="출력 HTML 경로")
    args = ap.parse_args()

    if args.mode in ("combo", "combo3"):
        kpp = args.kw_per_prod if args.mode == "combo3" else 0
        g = build_combo(args.seed, args.top_kw, args.ex_per_kw, args.rel_cutoff,
                        kw_per_prod=kpp, leaf_rel=args.leaf_rel)
        prefix = "combo3" if args.mode == "combo3" else "combo"
        out = args.out or os.path.join(OUT_DIR, f"{prefix}_{args.seed}.html")
        path = render_combo(g, out)
        print(f"시드: {args.seed} → 유의미 조합 {len(g['combos'])}개 (빈도 보정)")
        for c in g["combos"]:
            ex = " / ".join(f"{g['pn'][j]}({pj:.2f})" for j, _, pj in c["examples"])
            print(f"  《{g['kw_ids'][c['kt']]}》 score={c['score']:.5f} "
                  f"df={c['df']}  → {ex}")
        if g.get("leaves"):
            print(f"\nL3 제품 유효키워드 ({len(g['leaves'])}개 제품):")
            for j, lst in g["leaves"].items():
                kws = ", ".join(g["kw_ids"][kt] for kt, _ in lst)
                print(f"  [{g['pn'][j]}] → {kws}")
        print(f"\nsaved: {path}")
        return

    if args.mode == "chain":
        g = build_chain(args.seed, args.steps)
        out = args.out or os.path.join(OUT_DIR, f"chain_{args.seed}.html")
        path = render_chain(g, out)
        # 텍스트 사슬 출력
        parts = []
        for node in g["path"]:
            if node[0] == "kw":
                parts.append(f"《{g['kw_ids'][node[1]]}》")
            else:
                parts.append(f"[{g['pn'][node[1]]}]")
            if node[2] is not None:
                parts[-1] = f"--{node[2]:.3f}-->  " + parts[-1]
        print(f"시드: {args.seed}")
        print("  " + "  ".join(parts))
        print(f"\nsaved: {path}")
        return

    g = build_ego(args.seed, args.top_prod, args.top_kw)
    out = args.out or os.path.join(OUT_DIR, f"ego_{args.seed}.html")
    path = render(g, out)
    print(f"시드: {args.seed}")
    print(f"제품(조합 예시) {len(g['prods'])}개:")
    for j, att_in, pj in g["prods"]:
        print(f"  - {g['pn'][j]}  (성공확률 {pj:.3f}, att {att_in:.4f})")
    print(f"조합 키워드 top{len(g['top_kt'])}:")
    for kt, sc in g["top_kt"]:
        print(f"  - {g['kw_ids'][kt]}  ({sc:.5f})")
    print(f"\nsaved: {path}")


if __name__ == "__main__":
    main()
