"""sim 物理パラメータの較正（Optuna TPE＋物理prior bounds＋time-series val）。

【規律（最重要）】目的関数は「集約された創発統計を実測に一致させる」のみ。個々の着順予測 logloss や
回収率は目的にしない（＝物理定数の較正であって予測器フィッティングではない。市場エッジは対象外）。

較正する物理定数（SimConfig の knob。field_from_featured が作る RaceField=データ由来は較正しない）:
  turn_k / lane_swing_rate / lane_return（走行距離ロス）, kickback_k（砂被り）,
  gate_early（枠順優位）, going_speed_k / going_apt_gain（馬場）, turn_gain（回り適性）
  ※落馬(fall_*)は DNF率データが要るため本較正の目的統計に含めず、物理prior既定で固定。

目的統計（sim を実測に「一致」させる／一部は「最大化」）:
  - corr(脚質, 序盤位置)      : 実測(≈0.37)に一致  |sim−real| 最小（過決定を実測へ緩める）
  - corr(枠順, 着順)          : 実測の内枠有利に一致 |sim−real| 最小
  - backness_signal(展開機構) : 実測に一致          |sim−real| 最小
  - corr(sim序盤位置, 実第1コーナー) : 最大化（直接の位置一致）
  - pace_shape_corr           : 最大化（race_pace があれば）

プロトコル: train(≤train_max_year) で最小化 → val(≥val_min_year) で符号・概形の汎化を確認。
best＋近傍trial幅(不確実性)＋train/val統計を models/sim_calibration.json に保存。

実行例: python calibrate_sim.py --limit 1500 --n-sim 150 --trials 50 --train-max-year 2016 --val-min-year 2017
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 較正パラメータの物理prior bounds（この範囲外は探索しない＝弱い事前分布）
PARAM_BOUNDS = {
    "turn_k": (0.0, 0.03),
    "lane_swing_rate": (0.05, 0.40),
    "lane_return": (0.0, 0.15),
    "kickback_k": (0.0, 0.80),
    "gate_early": (0.0, 0.30),
    "going_speed_k": (0.0, 0.15),
    "going_apt_gain": (0.0, 0.15),
    "turn_gain": (0.0, 0.15),
    # 大域ノイズ lever（過決定＝style_pos/draw_bias の一律増幅 を実測へ下げる）:
    "noise_mult": (0.5, 3.0),        # 加速度ノイズ倍率（序盤位置の散り＝style_pos に効く）
    "ability_sigma": (0.10, 0.80),   # 能力の per-sim ばらつき（着順の散り＝draw_bias に効く）
}

# SimConfig のフィールドでない（monte_carlo 引数の）較正パラメータ。cfg 生成時に分離する。
_NON_CFG_PARAMS = ("ability_sigma",)

# 目的の重み（一致項は |sim-real|、最大化項は係数付きで負に加える）。
# 初回較正で draw_bias が実測の約2倍に過剰化し、非有界な最大化項(pos_direct/pace_shape)が
# パラメータを物理天井へ押し上げた反省から、draw_bias 一致を強く罰し・最大化項を弱めた既定。
# すべて CLI(--w-*)で上書き可能。
WEIGHTS = {
    "style_pos_match": 1.0,     # |sim-real| corr(脚質,序盤位置)
    "draw_bias_match": 3.0,     # |sim-real| corr(枠順,着順)  ← 過剰を強く罰する
    "backness_match": 0.5,      # |sim-real| 展開signal
    "pos_direct_max": 0.5,      # -corr(sim序盤位置, 実第1コーナー)  ← 天井押し上げを緩和
    "pace_shape_max": 0.3,      # -pace_shape_corr               ← 同上
}


def _spearman(a, b):
    import numpy as np
    import pandas as pd
    a = np.asarray(a, float); b = np.asarray(b, float)
    m = np.isfinite(a) & np.isfinite(b)
    if int(m.sum()) < 5:
        return float("nan")
    ra = pd.Series(a[m]).rank().to_numpy(); rb = pd.Series(b[m]).rank().to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def real_stats(rows: dict) -> dict:
    """実測の集約統計（1回だけ計算）。rows は列ごとの np 配列（プール済み・馬単位）。"""
    from src.simulation._fidelity import pace_backness_signal
    out = {
        "style_pos": _spearman(rows["style"], rows["real_pos"]),   # 脚質×実第1コーナー
        "draw_bias": _spearman(rows["draw"], rows["rank"]),        # 枠順×着順
    }
    if rows.get("pace") is not None:
        sig = pace_backness_signal(rows["backness"], rows["rank_norm"], rows["pace"])
        out["backness"] = sig["signal"]
    return out


def objective_distance(sim_stats: dict, real: dict, has_pace: bool, weights: dict | None = None) -> float:
    """集約統計の距離（最小化対象）。一致項＋最大化項（負）。weights 省略時は既定 WEIGHTS。"""
    import numpy as np
    w = weights or WEIGHTS
    d = 0.0

    def _absdiff(k):
        a, b = sim_stats.get(k), real.get(k)
        return abs(a - b) if (a is not None and b is not None and np.isfinite(a) and np.isfinite(b)) else 0.0
    d += w["style_pos_match"] * _absdiff("style_pos")
    d += w["draw_bias_match"] * _absdiff("draw_bias")
    if "backness" in real:
        d += w["backness_match"] * _absdiff("backness")
    pm = sim_stats.get("pos_direct")
    if pm is not None and np.isfinite(pm):
        d -= w["pos_direct_max"] * pm
    if has_pace:
        ps = sim_stats.get("pace_shape")
        if ps is not None and np.isfinite(ps):
            d -= w["pace_shape_max"] * ps
    return float(d)


def main():
    ap = argparse.ArgumentParser(description="sim 物理パラメータの Optuna 較正（集約統計への一致）")
    ap.add_argument("--limit", type=int, default=1500, help="train/val 各区間のサンプルレース上限")
    ap.add_argument("--n-sim", type=int, default=150)
    ap.add_argument("--trials", type=int, default=50)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--train-max-year", type=int, default=2016)
    ap.add_argument("--val-min-year", type=int, default=2017)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", type=str, default="models/sim_calibration.json")
    # 目的の重み（既定は WEIGHTS。draw_bias 過剰・天井張り付きの再調整用に個別上書き可能）
    ap.add_argument("--w-style-pos", type=float, default=WEIGHTS["style_pos_match"])
    ap.add_argument("--w-draw-bias", type=float, default=WEIGHTS["draw_bias_match"])
    ap.add_argument("--w-backness", type=float, default=WEIGHTS["backness_match"])
    ap.add_argument("--w-pos-direct", type=float, default=WEIGHTS["pos_direct_max"])
    ap.add_argument("--w-pace-shape", type=float, default=WEIGHTS["pace_shape_max"])
    args = ap.parse_args()
    weights = {"style_pos_match": args.w_style_pos, "draw_bias_match": args.w_draw_bias,
               "backness_match": args.w_backness, "pos_direct_max": args.w_pos_direct,
               "pace_shape_max": args.w_pace_shape}
    print(f"[weights] {weights}")

    import numpy as np
    import pandas as pd

    try:
        import optuna
    except ImportError:
        print("optuna 未導入。pip install optuna で導入してください。"); return

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.preprocessing._horse_results_processor import parse_corner
    from src.simulation._agent_race import SimConfig, monte_carlo
    from src.simulation._fidelity import pace_backness_signal, pace_shape_corr
    from src.simulation._sim_params import field_from_featured

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません"); return
    if "通過" not in featured.columns:
        print("featured に通過列が無い（位置忠実度が測れない）。rebuild-featured 後に。"); return

    pace_path = Path(LocalPaths.RAW_DIR) / "race_pace.pkl"
    race_pace = None
    if pace_path.exists():
        rp = pd.read_pickle(pace_path)
        race_pace = dict(zip(rp["race_id"].astype(str), pd.to_numeric(rp["pace_diff"], errors="coerce")))

    yr = pd.to_datetime(featured["date"], errors="coerce").dt.year
    date = pd.to_datetime(featured["date"], errors="coerce").groupby(level=0).first().sort_values()

    def _races(mask_years) -> list:
        ids = [r for r in date.index if r in mask_years.index and bool(mask_years.loc[r])]
        return ids[-args.limit:] if len(ids) > args.limit else ids

    yr_by_race = yr.groupby(level=0).first()
    train_ids = _races(yr_by_race <= args.train_max_year)
    val_ids = _races(yr_by_race >= args.val_min_year)
    print(f"[split] train={len(train_ids):,}レース(≤{args.train_max_year}) / "
          f"val={len(val_ids):,}レース(≥{args.val_min_year}) / race_pace={'有' if race_pace else '無'}")
    if not train_ids or not val_ids:
        print("train/val のどちらかが空。--train-max-year/--val-min-year を調整。"); return

    def _collect_real(ids):
        """実測プール（馬単位）を1回だけ作る。"""
        style, real_pos, rank, draw, backness, rank_norm, pace = [], [], [], [], [], [], []
        for rid in ids:
            rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
            if len(rd) < 4:
                continue
            rk = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
            if not np.isfinite(rk).all():
                continue
            nH = len(rd)
            field = field_from_featured(rd, ability_spread=args.ability_spread)
            fc = pd.to_numeric(rd["通過"].map(lambda x: parse_corner(x, 1)), errors="coerce").to_numpy()
            dr = pd.to_numeric(rd.get(ResultsCols.UMABAN), errors="coerce").to_numpy() if ResultsCols.UMABAN in rd.columns else field.gate
            lt = pd.to_numeric(rd["leg_type_binary"], errors="coerce").to_numpy() if "leg_type_binary" in rd.columns else np.full(nH, np.nan)
            rp_ = race_pace.get(str(rid), np.nan) if race_pace else np.nan
            for i in range(nH):
                style.append(field.style[i]); real_pos.append((fc[i]-1)/max(nH-1, 1) if np.isfinite(fc[i]) else np.nan)
                rank.append(rk[i]); draw.append(dr[i]); rank_norm.append((rk[i]-1)/max(nH-1, 1))
                b = lt[i] if np.isfinite(lt[i]) else field.style[i]/2.0
                backness.append(b); pace.append(rp_)
        return dict(style=np.array(style), real_pos=np.array(real_pos), rank=np.array(rank),
                    draw=np.array(draw), backness=np.array(backness), rank_norm=np.array(rank_norm),
                    pace=np.array(pace) if race_pace else None)

    real_train = _collect_real(train_ids)
    real_val = _collect_real(val_ids)
    R_train = real_stats(real_train)
    R_val = real_stats(real_val)
    print(f"[real train] {R_train}")

    # sim を回し、実測と同順で対応させた集約統計を返す（pos_direct=sim位置 vs 実第1コーナー）。
    def evaluate(ids, cfg, ability_sigma, seed):
        rng = np.random.default_rng(seed)
        s_style, s_pos, s_rank, s_draw, s_back, s_rn = [], [], [], [], [], []
        r_pos, r_pace = [], []
        sim_pace_r, real_pace_r = [], []
        for rid in ids:
            rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
            if len(rd) < 4:
                continue
            rk = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
            if not np.isfinite(rk).all():
                continue
            nH = len(rd)
            field = field_from_featured(rd, ability_spread=args.ability_spread)
            sim = monte_carlo(field, n_sim=args.n_sim, cfg=cfg, seed=int(rng.integers(1 << 30)),
                              ability_sigma=ability_sigma, track_dynamics=True)
            epr = sim["early_pos_rank"]; smr = sim["mean_rank"]
            fc = pd.to_numeric(rd["通過"].map(lambda x: parse_corner(x, 1)), errors="coerce").to_numpy()
            dr = pd.to_numeric(rd.get(ResultsCols.UMABAN), errors="coerce").to_numpy() if ResultsCols.UMABAN in rd.columns else field.gate
            lt = pd.to_numeric(rd["leg_type_binary"], errors="coerce").to_numpy() if "leg_type_binary" in rd.columns else np.full(nH, np.nan)
            rp_ = race_pace.get(str(rid), np.nan) if race_pace else np.nan
            denom = max(nH-1, 1)
            for i in range(nH):
                s_style.append(field.style[i]); s_pos.append(epr[i]/denom)
                s_rank.append(smr[i]); s_draw.append(dr[i]); s_rn.append((smr[i]-1)/denom)
                r_pos.append((fc[i]-1)/denom if np.isfinite(fc[i]) else np.nan)
                b = lt[i] if np.isfinite(lt[i]) else field.style[i]/2.0
                s_back.append(b); r_pace.append(rp_)
            if race_pace and np.isfinite(rp_):
                sp = (sim["early_speed"]-sim["late_speed"])/(sim["early_speed"]+sim["late_speed"]+1e-9)
                sim_pace_r.append(sp); real_pace_r.append(rp_)
        st = {"style_pos": _spearman(s_style, s_pos),
              "draw_bias": _spearman(s_draw, s_rank),
              "pos_direct": _spearman(s_pos, r_pos)}
        if race_pace:
            st["pace_shape"] = pace_shape_corr(sim_pace_r, real_pace_r) if sim_pace_r else float("nan")
            st["backness"] = pace_backness_signal(s_back, s_rn, r_pace)["signal"]
        return st

    def split_params(params):
        """(SimConfig 用 cfg_params, ability_sigma) に分離する。"""
        cfg_params = {k: v for k, v in params.items() if k not in _NON_CFG_PARAMS}
        return cfg_params, params.get("ability_sigma", args.ability_sigma)

    def make_cfg(params):
        cfg_params, _ = split_params(params)
        return SimConfig(T=args.T, **cfg_params)

    def suggest(trial):
        return {k: trial.suggest_float(k, lo, hi) for k, (lo, hi) in PARAM_BOUNDS.items()}

    def obj(trial):
        params = suggest(trial)
        _, absig = split_params(params)
        st = evaluate(train_ids, make_cfg(params), absig, seed=args.seed)
        return objective_distance(st, R_train, has_pace=race_pace is not None, weights=weights)

    sampler = optuna.samplers.TPESampler(seed=args.seed)
    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(obj, n_trials=args.trials, show_progress_bar=False)

    best = study.best_params
    # 近傍trial幅（"貧者の事後分布"）: 最良から上位20%の trial のパラメータ分位
    trials_df = study.trials_dataframe()
    ok = trials_df[trials_df["value"].notna()].sort_values("value")
    top = ok.head(max(3, len(ok) // 5))
    spread = {k: [float(top[f"params_{k}"].quantile(0.1)), float(top[f"params_{k}"].quantile(0.9))]
              for k in PARAM_BOUNDS}

    _, best_absig = split_params(best)
    st_train = evaluate(train_ids, make_cfg(best), best_absig, seed=args.seed + 1)
    st_val = evaluate(val_ids, make_cfg(best), best_absig, seed=args.seed + 2)

    print("\n[best params]")
    for k in PARAM_BOUNDS:
        print(f"  {k:<16} = {best[k]:.4f}   （上位20%幅 {spread[k][0]:.4f}–{spread[k][1]:.4f}）")
    print("\n[汎化確認] 集約統計 sim(best) vs 実測")
    print(f"  {'統計':<12}{'train_sim':>11}{'train_real':>11}{'val_sim':>11}{'val_real':>11}")
    for k in ("style_pos", "draw_bias", "backness", "pos_direct", "pace_shape"):
        ts = st_train.get(k); vs = st_val.get(k)
        tr = R_train.get(k); vr = R_val.get(k)
        def _f(x):
            return f"{x:+.3f}" if (x is not None and np.isfinite(x)) else "   -  "
        print(f"  {k:<12}{_f(ts):>11}{_f(tr):>11}{_f(vs):>11}{_f(vr):>11}")
    print("\n判定: style_pos/draw_bias/backness が val でも実測に近く、pos_direct/pace_shape が val で正なら"
          "\n      物理定数はデータで裏づけられ汎化している（符号一致まで主張・magnitude は幅で報告）。")

    out = {"best_params": best, "top20_spread": spread,
           "train_stats": st_train, "val_stats": st_val,
           "real_train": R_train, "real_val": R_val, "weights": weights,
           "config": {"limit": args.limit, "n_sim": args.n_sim, "trials": args.trials,
                      "train_max_year": args.train_max_year, "val_min_year": args.val_min_year}}
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n保存: {outp}")


if __name__ == "__main__":
    main()
