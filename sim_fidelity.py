"""忠実度検証ハーネス: sim の創発ダイナミクスが実測分布と一致するかを測る。

sim を「予測器」でなく「レースの物理モデル」として評価する。3つの無次元メトリクスを、
**実測(laptime/results由来) と sim の双方**で計算し、一致度を見る:

  (1) pace_shape   : sim ペース(序盤−終盤速度) vs 実ペース(前半3F−上がり3F) の順位相関。
                     sim が『どのレースが前傾/ハイペースか』を再現できるか。
  (2) pace→展開    : 「ハイペース→後方脚質が前」機構の signal を実/simで比較（機構の再現）。
  (3) position     : 序盤の位置順位と脚質の相関を実(通過)/sim(early_pos_rank)で比較。

前提: race_pace.pkl（`import_archive_laptime.py` で作成）が要る。無ければ (1)(2) はスキップ。
実行例: python sim_fidelity.py --limit 6000 --n-sim 400
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    ap = argparse.ArgumentParser(description="ABS 忠実度検証（創発ダイナミクス vs 実測）")
    ap.add_argument("--limit", type=int, default=6000)
    ap.add_argument("--max-year", type=int, default=None)
    ap.add_argument("--n-sim", type=int, default=400)
    ap.add_argument("--T", type=int, default=100,
                    help="レース総時間（dt=1.0 換算のステップ数）。実ステップ数は round(T/dt)。")
    ap.add_argument("--dt", type=float, default=1.0,
                    help="時間刻み。細かいほど一瞬の駆け引きを解像（総時間 T·dt は保存）。"
                         "1d/2d 両エンジンとも dt 不変(√dtノイズ)で収束。計算量 ∝ 1/dt。")
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--engine", choices=["1d", "2d"], default="1d",
                    help="1d=既存物理 / 2d=Phase1.5（発走速度・位置ターゲット・2次元位置取り）")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--calibrated", action="store_true",
                    help="models/sim_calibration.json の best_params(較正済み物理定数)を適用して測る。")
    ap.add_argument("--course-env", action="store_true",
                    help="Phase A: レース毎に course_* 幾何で SimConfig を上書き（幅員→有効幅/高低差→"
                         "消耗/直線→終盤到達/曲率→turn_k）。幾何欠損レースは base のまま＝後方互換。")
    ap.add_argument("--course-affinity", action="store_true",
                    help="Phase B: 出走馬×コース相性で RaceField.ability を馬別補正（脚質バイアス×脚質/"
                         "時計傾向×スピード型）。プロファイル欠損は base のまま＝後方互換。")
    ap.add_argument("--override", nargs="*", default=[], metavar="KEY=VAL",
                    help="較正/既定パラメータを個別上書き（アブレーション用）。"
                         "例: --override noise_mult=1.0 pos_gain=0.6")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import SimConfig, monte_carlo
    from src.simulation._fidelity import pace_backness_signal, pace_shape_corr, spearman
    from src.simulation._sim_params import field_from_featured

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return

    pace_path = Path(LocalPaths.RAW_DIR) / "race_pace.pkl"
    race_pace = None
    if pace_path.exists():
        rp = pd.read_pickle(pace_path)
        race_pace = dict(zip(rp["race_id"].astype(str),
                             pd.to_numeric(rp["pace_diff"], errors="coerce")))
        print(f"[race_pace] {len(race_pace):,} レースのペースをロード（前傾ほど pace_diff 大）")
    else:
        print(f"[race_pace] {pace_path} が無いので pace 系メトリクスはスキップ。"
              "import_archive_laptime.py で作成してください。")

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(date.index)
    if args.max_year:
        order = [r for r in order if str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]
    featured = featured.loc[order]
    # 総時間 T·dt を保存したまま dt を細かくする: 実ステップ数 = round(T / dt)。
    steps = max(1, round(args.T / args.dt))
    import dataclasses

    def _load_calibration(cfg_cls, fname):
        """--calibrated 時に models/<fname> の best_params を読み、cfg クラスの有効フィールドだけ
        適用（他エンジンの knob は無視）。ability_sigma は monte_carlo 引数なので分離して返す。"""
        cal_params, eff_absig = {}, args.ability_sigma
        if not args.calibrated:
            return cal_params, eff_absig
        cal_path = Path(__file__).resolve().parent / "models" / fname
        if not cal_path.exists():
            print(f"[calibrated] {cal_path} が無い。calibrate_sim.py を先に実行。既定パラメータで続行。")
            return cal_params, eff_absig
        import json
        raw = dict(json.loads(cal_path.read_text()).get("best_params", {}))
        eff_absig = raw.pop("ability_sigma", eff_absig)
        valid = {f.name for f in dataclasses.fields(cfg_cls)}
        cal_params = {k: v for k, v in raw.items() if k in valid}
        print(f"[calibrated] {cal_path.name}: "
              + ", ".join(f"{k}={v:.4f}" for k, v in cal_params.items())
              + f", ability_sigma={eff_absig:.4f}")
        return cal_params, eff_absig

    def _apply_overrides(cal_params, eff_absig, cfg_cls):
        """--override KEY=VAL を較正/既定パラメータへ上書き（アブレーション用）。
        ability_sigma は monte_carlo 引数なので別扱い。無効キーは警告して無視。"""
        if not args.override:
            return cal_params, eff_absig
        valid = {f.name for f in dataclasses.fields(cfg_cls)}
        for kv in args.override:
            k, _, v = kv.partition("=")
            fv = float(v)
            if k == "ability_sigma":
                eff_absig = fv
            elif k in valid:
                cal_params[k] = fv
            else:
                print(f"[override] 無効なキー {k} を無視（{cfg_cls.__name__} に無い）")
        print("[override] " + ", ".join(args.override))
        return cal_params, eff_absig

    if args.engine == "2d":
        from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d
        cal_params, eff_ability_sigma = _load_calibration(SimConfig2D, "sim_calibration_2d.json")
        cal_params, eff_ability_sigma = _apply_overrides(cal_params, eff_ability_sigma, SimConfig2D)
        cfg = SimConfig2D(T=steps, dt=args.dt, **cal_params)
        run_sim = lambda fld, sd, c=None: monte_carlo_2d(  # noqa: E731
            fld, n_sim=args.n_sim, cfg=c or cfg, seed=sd,
            ability_sigma=eff_ability_sigma, track_dynamics=True)
    else:
        cal_params, eff_ability_sigma = _load_calibration(SimConfig, "sim_calibration.json")
        cal_params, eff_ability_sigma = _apply_overrides(cal_params, eff_ability_sigma, SimConfig)
        cfg = SimConfig(T=steps, dt=args.dt, **cal_params)
        run_sim = lambda fld, sd, c=None: monte_carlo(  # noqa: E731
            fld, n_sim=args.n_sim, cfg=c or cfg, seed=sd,
            ability_sigma=eff_ability_sigma, track_dynamics=True)
    # Phase A/B: どちらかが on なら course_* をレース毎に解決（CourseContext を両者で共有）。
    course_params = affinity_params = None
    use_course = args.course_env or args.course_affinity
    if use_course:
        from src.simulation._course_affinity import (
            course_affinity_params_from_mapping,
            field_for_course,
        )
        from src.simulation._course_env import (
            course_context_from_featured,
            course_env_params_from_mapping,
            sim_config_for_course,
        )
        raw = {}
        if args.calibrated:   # 較正済み ce_*/ca_* ゲインがあれば使う（無ければ各既定）
            fname = "sim_calibration_2d.json" if args.engine == "2d" else "sim_calibration.json"
            calp = Path(__file__).resolve().parent / "models" / fname
            if calp.exists():
                import json
                raw = dict(json.loads(calp.read_text()).get("best_params", {}))
        if args.course_env:
            course_params = course_env_params_from_mapping(raw)
            print(f"[course-env] Phase A: course_* 幾何で SimConfig 上書き"
                  f"（{'較正ゲイン' if course_params else '既定ゲイン'}・幾何欠損は base）。")
        if args.course_affinity:
            affinity_params = course_affinity_params_from_mapping(raw)
            print(f"[course-affinity] Phase B: 出走馬×コース相性で ability 補正"
                  f"（{'較正ゲイン' if affinity_params else '既定ゲイン'}・プロファイル欠損は base）。")
    print(f"[engine] {args.engine} / dt={args.dt} / 実ステップ数={steps}（総時間 T·dt={args.T}）"
          "  ※1d/2d とも dt 不変（ノイズ√dt）")
    rng = np.random.default_rng(args.seed)

    # 収集: レース単位（sim/実ペース）と 馬単位（脚質・相対着順・序盤位置）
    sim_pace_r, real_pace_r = [], []
    backness, rank_norm = [], []        # 馬単位（展開signal用）
    real_pace_perrow = []
    pos_style, pos_sim_early, pos_real_corner = [], [], []   # 位置忠実度用

    has_corner = "通過" in featured.columns
    for rid in order:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        if len(rd) < 4:
            continue
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        nH = len(rd)
        lt = pd.to_numeric(rd["leg_type_binary"], errors="coerce").to_numpy() if "leg_type_binary" in rd.columns else np.full(nH, np.nan)
        if not np.isfinite(rank).all():
            continue

        field = field_from_featured(rd, ability_spread=args.ability_spread)
        ctx = course_context_from_featured(rd) if use_course else None
        if args.course_affinity:                     # Phase B: 馬別 ability 相性補正
            field = field_for_course(field, rd, ctx, affinity_params)
        cfg_r = sim_config_for_course(cfg, ctx, course_params) if args.course_env else None
        sim = run_sim(field, int(rng.integers(1 << 30)), cfg_r)
        # sim 前傾度は速度レベルで正規化（(early-late)/(early+late)）＝形だけ測り能力レベル交絡を除く
        _es, _ls = sim["early_speed"], sim["late_speed"]
        sp = (_es - _ls) / (_es + _ls + 1e-9)

        rp = race_pace.get(str(rid), np.nan) if race_pace is not None else np.nan
        if np.isfinite(rp):
            sim_pace_r.append(sp)
            real_pace_r.append(rp)

        rn = (rank - 1) / max(nH - 1, 1)                # 相対着順 0..1
        # backness: 実 leg_type_binary（0先行..1追込）。無い馬は sim style で代用
        b = lt.copy()
        nanb = ~np.isfinite(b)
        if nanb.any():
            b[nanb] = field.style[nanb] / 2.0
        for i in range(nH):
            backness.append(b[i]); rank_norm.append(rn[i]); real_pace_perrow.append(rp)

        # 位置忠実度: sim 序盤位置順位 vs 実 第1コーナー（頭数で [0,1] 正規化してプール）
        if has_corner:
            from src.preprocessing._horse_results_processor import parse_corner
            fc = pd.to_numeric(rd["通過"].map(lambda x: parse_corner(x, 1)), errors="coerce").to_numpy()
            denom = max(nH - 1, 1)
            for i in range(nH):
                pos_style.append(field.style[i])
                pos_sim_early.append(sim["early_pos_rank"][i] / denom)          # 0=先頭..1=最後方
                pos_real_corner.append((fc[i] - 1) / denom if np.isfinite(fc[i]) else np.nan)

    print("=" * 72)
    print(f"ABS 忠実度検証 / {len(order):,}レース / n_sim={args.n_sim}")
    print("-" * 72)

    if sim_pace_r:
        c = pace_shape_corr(sim_pace_r, real_pace_r)
        print(f"(1) ペース形 忠実度: corr(sim前傾度, 実前傾度) = {c:+.3f}   "
              f"[{len(sim_pace_r):,}レース]")
        print("     → 正で大きいほど『どのレースが前傾か』を sim が再現。0付近なら再現できず。")

    if race_pace is not None and len(backness) > 100:
        real_sig = pace_backness_signal(backness, rank_norm, real_pace_perrow)
        # sim 側の展開signal: sim 平均着順を相対化し、同じ実ペースで分割
        # （sim の着順を使い、機構が実と同じ向きに出るかを見る）
        print(f"(2) 展開機構 忠実度（ハイペース→後方脚質が前）:")
        print(f"     実測 signal = {real_sig['signal']:+.3f}  "
              f"(corr_hi={real_sig['corr_hi']:+.3f}, corr_lo={real_sig['corr_lo']:+.3f})")
        print("     → 正なら『実データでも展開機構が存在』。0/負なら展開効果は弱い/逆。")

    if has_corner and len(pos_style) > 100:
        cs = spearman(pos_style, pos_sim_early)
        cr = spearman(pos_style, pos_real_corner)
        c_direct = spearman(pos_sim_early, pos_real_corner)   # sim位置 vs 実位置（直接）
        print(f"(3) 隊列 忠実度: corr(脚質, 序盤位置)  sim={cs:+.3f}  実={cr:+.3f}")
        print(f"(3b) 位置 直接一致: corr(sim序盤位置, 実第1コーナー) = {c_direct:+.3f}")
        print("     → (3b)が正で大きいほど『実際に前にいた馬』を sim も前に置けている（直接の隊列再現）。")
    elif not has_corner:
        print("(3) 隊列 忠実度: featured に通過列が無いためスキップ"
              "（leg_type修正版で rebuild-featured 後に有効）。")

    print("-" * 72)
    print("解釈: (1)(3) が正で大きく、(2)の実測signalが正なら、sim は展開・隊列を物理として")
    print("再現できている。ここで初めて『連系の依存構造 vs 市場』へ進む価値が判定できる。")
    print("=" * 72)


if __name__ == "__main__":
    main()
