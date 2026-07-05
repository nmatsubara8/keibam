"""複勝（FUKUSHO）overlay バックテスト — walk-forward OOS で「市場と戦えるか」を測る。

背景（.claude-context §市場効率）:
  単勝市場は効率的で独立エッジ未確認。ただし複勝だけは非単勝の中で唯一、
  (1) 控除率が最安（20%、単勝と同率。連系は 22.5〜25%）
  (2) Harville の市場焼き直しでなく **Place ヘッドが top3 を直接学習** した信号を使える
  という点で「エッジの経路が理屈で通る」券種。本スクリプトはそれを OOS で検証する。

この検証が既存の app 5_backtest「2ヘッド+確定オッズ」タブに足すもの:
  A) **payout≥1.5 フィルタ**（--min-odds）: 本命の複勝は 1.1 倍程度しか付かず、控除率20%を
     的中率で覆せない（1.1 倍を勝つには ~91% 的中が必要）。妙味が残る中穴ゾーンに絞る。
  B) **overlay>0 ゲート**（既定 ON / --no-overlay-gate で無効）: place_overlay = 複勝市場の
     implied − Harville理論。>0 は「市場が複勝を過小評価」＝我々が市場を *なぞる* のでなく
     市場のズレを突く方向。Place ヘッドが fukusho_implied_p で市場 echo する懸念への対策。
  C) **年ごとの walk-forward** 表示: フィルタ有無を年別に A/B で並べ、フロック感度（除外後ROI）も出す。

リーク注意（CLI backtest と同じ）: 確定オッズ・市場歪み特徴は発走前の確定値で結果非依存。
ただし **モデルの学習年に評価年を含めない**こと。--years で評価年を学習年と重ねずに指定する。

実行例（ローカル、featured_data / models / return_tables が揃った環境で）:
    python verify_fukusho_overlay_backtest.py --years 2025 2026 --min-odds 1.5
    python verify_fukusho_overlay_backtest.py --years 2026 --no-overlay-gate   # A だけ効かせる
"""

from __future__ import annotations

import argparse

import pandas as pd

from src.constants._bet_types import BetType
from src.constants._bet_thresholds import MIN_BETS_FOR_RELIABLE_STAT
from src.constants._results_cols import ResultsCols
from src.simulation._backtest import BetTypeStats
from src.simulation._backtest import default_thresholds
from src.simulation._backtest import select_candidates
from src.simulation._backtest import settle_candidates

_OVERLAY_COL = "place_overlay"


def _load_all():
    """featured_data / Place・Win ヘッド / 払戻テーブル / 確定オッズ lookup を読む。

    app 5_backtest と同じローダを使い、UI と同一の入力で評価する。
    いずれか欠損時は None を返し、呼び出し側が理由を表示して終了する。
    """
    from app._data_loader import find_model_paths
    from app._data_loader import load_model_from_path
    from app._data_loader import load_odds_snapshots
    from app._data_loader import load_win_head_for
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.preparing._odds_snapshot import build_final_odds_lookup

    featured = load_featured_data()
    paths = find_model_paths("models")
    return {
        "featured": featured,
        "model_paths": paths,
        "load_model_from_path": load_model_from_path,
        "load_win_head_for": load_win_head_for,
        "return_processor": _load_return_processor(),
        "final_odds_lookup": (
            build_final_odds_lookup(snaps) if (snaps := load_odds_snapshots()) else None
        ),
    }


def _resolve_model_path(paths: list[str], version: str | None) -> str | None:
    """--model-version が指定されればその版を、無ければ先頭（最新）を返す。"""
    if not paths:
        return None
    if version is None:
        return paths[0]
    import os

    for p in paths:
        if version in os.path.basename(p):
            return p
    return None


def _build_overlay_lookup(featured: pd.DataFrame) -> dict | None:
    """{(race_id, 馬番): place_overlay} を featured から作る。列が無ければ None。"""
    if _OVERLAY_COL not in featured.columns or ResultsCols.UMABAN not in featured.columns:
        return None
    rid = featured.index.astype(str)
    lookup: dict = {}
    for r, uma, ov in zip(rid, featured[ResultsCols.UMABAN], featured[_OVERLAY_COL]):
        u = pd.to_numeric(uma, errors="coerce")
        if pd.isna(u):
            continue
        lookup[(r, int(u))] = ov
    return lookup


def _filter_candidates(candidates, *, min_odds, overlay_lookup):
    """複勝候補に payout≥min_odds と（overlay_lookup があれば）overlay>0 ゲートを適用する。

    overlay が NaN / 未収載（確定オッズ無し）の馬はゲートを **通さない**（保守的）。
    """
    out = []
    for c in candidates:
        if c.bet_type != BetType.FUKUSHO:
            continue
        if c.odds < min_odds:
            continue
        if overlay_lookup is not None:
            ov = overlay_lookup.get((str(c.race_id), int(c.combo[0])))
            if ov is None or not (ov > 0):  # NaN も False
                continue
        out.append(c)
    return out


def _fukusho_stats(candidates, return_processor, unit) -> BetTypeStats:
    """候補を決済し複勝の BetTypeStats を返す（空なら空の stats）。"""
    per = settle_candidates(candidates, return_processor, unit=unit)
    return per.get(BetType.FUKUSHO, BetTypeStats(BetType.FUKUSHO))


def _fmt(s: BetTypeStats) -> str:
    """1 行分の指標を整形（点数/的中/的中率/回収率/除外後回収率/信頼マーク）。"""
    mark = "✓" if s.reliable else f"参考(<{MIN_BETS_FOR_RELIABLE_STAT})"
    return (
        f"{s.n_bets:>6d}{s.n_hits:>6d}{s.hit_rate:>8.1%}"
        f"{s.roi:>9.1%}{s.roi_ex_top:>9.1%}  {mark}"
    )


def run(args) -> int:
    data = _load_all()
    featured = data["featured"]
    if featured is None or featured.empty:
        print("[NG] featured_data が読めません。先に取込・特徴量生成を実行してください。")
        return 2
    rp = data["return_processor"]
    if rp is None:
        print("[NG] 払戻テーブル（return_tables）が読めません。ingest で取得してください。")
        return 2
    model_path = _resolve_model_path(data["model_paths"], args.model_version)
    if model_path is None:
        print("[NG] モデルが見つかりません（models/）。先に retrain を実行してください。")
        return 2

    place_ai = data["load_model_from_path"](model_path)
    win_ai = data["load_win_head_for"](model_path)
    place_model = place_ai.effective_model
    win_model = win_ai.effective_model if win_ai is not None else None
    final_lookup = data["final_odds_lookup"]

    # overlay ゲート: 列が無ければ自動で無効化（payout フィルタだけ効く）。
    overlay_lookup = None
    if not args.no_overlay_gate:
        overlay_lookup = _build_overlay_lookup(featured)
        if overlay_lookup is None:
            print(f"[注意] featured に '{_OVERLAY_COL}' 列が無く overlay ゲートを無効化します"
                  "（確定オッズ由来の市場歪み特徴が未生成）。payout フィルタのみ適用。")

    # 評価年: 指定なければ featured の全年（walk-forward の責任は利用者に委ねる旨を明示）。
    years = args.years
    if not years:
        years = sorted({r[:4] for r in featured.index.astype(str) if r[:4].isdigit()}, reverse=True)
        print(f"[注意] --years 未指定。featured 全年 {years} を評価します。"
              "学習年と重なる年は楽観バイアス（リーク）になるため、"
              "本番判定では学習年より後の年だけを --years で指定してください。")

    # overlay カバレッジ診断: place_overlay は「複勝確定オッズ vs 単勝Harville」の差であり、
    # 定義上、複勝確定オッズを捕捉した行にしか存在しない（単勝だけからは作れない）。
    # ここで年別の非null率を先に出し、ゲートで買い目が消えたのが「市場ズレ無し」なのか
    # 「そもそも overlay 未収載（カバレッジ不足）」なのかを取り違えないようにする。
    if overlay_lookup is not None and _OVERLAY_COL in featured.columns:
        print("[overlay カバレッジ] place_overlay の年別 非null 率（低いとゲートが機能しない）:")
        rid_all = featured.index.astype(str)
        for y in (years or []):
            g = featured[rid_all.str[:4] == str(y)]
            if g.empty:
                continue
            ov = pd.to_numeric(g[_OVERLAY_COL], errors="coerce")
            nn = int(ov.notna().sum())
            pos = int((ov > 0).sum())
            share = (pos / nn * 100) if nn else 0.0
            flag = "  ← ほぼ空。overlay 検証は不能（複勝確定オッズの蓄積が必要）" if nn < 30 else ""
            print(f"    {y}: rows={len(g):>6}  非null={nn:>6}({nn/len(g)*100:4.1f}%)  "
                  f"うち>0={pos:>5}({share:4.1f}%){flag}")

    ev_thr = args.ev_threshold if args.ev_threshold is not None else default_thresholds().get(BetType.FUKUSHO, 1.0)
    thresholds = {BetType.FUKUSHO: ev_thr}
    gate_desc = "payout≥%.2f" % args.min_odds + ("" if overlay_lookup is None else " かつ overlay>0")

    print("=" * 78)
    print("複勝 overlay バックテスト（walk-forward OOS）")
    print(f"  モデル: {model_path}   Win ヘッド: {'有' if win_model is not None else '無'}"
          f"   確定オッズ: {len(final_lookup or {}):,} 件")
    print(f"  EV 閾値(複勝)={ev_thr}   フィルタ=[{gate_desc}]   unit={args.unit}")
    print("=" * 78)
    header = (f"{'年':<6}{'  区分':<10}"
             f"{'点数':>6}{'的中':>6}{'的中率':>8}{'回収率':>9}{'除外後':>9}  信頼")
    print(header)
    print("-" * len(header))

    pooled_base = BetTypeStats("ALL")
    pooled_filt = BetTypeStats("ALL")
    for y in years:
        rid = featured.index.astype(str)
        X = featured[rid.str[:4] == str(y)]
        if X.empty:
            continue
        candidates = select_candidates(
            place_model, X, win_model=win_model,
            final_odds_lookup=final_lookup, thresholds=thresholds, takeout=args.takeout,
        )
        base = _fukusho_stats(
            [c for c in candidates if c.bet_type == BetType.FUKUSHO], rp, args.unit
        )
        filt = _fukusho_stats(
            _filter_candidates(candidates, min_odds=args.min_odds, overlay_lookup=overlay_lookup),
            rp, args.unit,
        )
        print(f"{str(y):<6}{'  EV素通し':<10}{_fmt(base)}")
        print(f"{'':<6}{'  +フィルタ':<10}{_fmt(filt)}")
        for pooled, s in ((pooled_base, base), (pooled_filt, filt)):
            pooled.n_bets += s.n_bets
            pooled.n_hits += s.n_hits
            pooled.stake += s.stake
            pooled.returned += s.returned
            pooled.max_return = max(pooled.max_return, s.max_return)

    print("-" * len(header))
    print(f"{'合算':<6}{'  EV素通し':<10}{_fmt(pooled_base)}")
    print(f"{'':<6}{'  +フィルタ':<10}{_fmt(pooled_filt)}")
    print("=" * 78)
    print("判定の見方:")
    print("  ・回収率>100% が複数年で安定し、かつ的中数≥"
          f"{MIN_BETS_FOR_RELIABLE_STAT}（信頼✓）で初めて『複勝にエッジ』と言える。")
    print("  ・『除外後』（最大払戻1本を除いた回収率）が回収率と大きく乖離するなら")
    print("    フロック（万馬券1本依存）＝再現性が低い。複勝は乖離が小さいのが健全。")
    print("  ・+フィルタが EV素通しより点数を絞りつつ回収率を押し上げていれば、")
    print("    payout≥1.5 / overlay>0 の仮説（中穴×市場ズレ）が効いている証拠。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="複勝 overlay walk-forward バックテスト")
    ap.add_argument("--years", nargs="*", default=None,
                    help="評価年（例: --years 2025 2026）。未指定なら全年（学習年と重ねない責任は利用者）")
    ap.add_argument("--min-odds", type=float, default=1.5,
                    help="複勝 payout 下限（本命除外。既定1.5）")
    ap.add_argument("--no-overlay-gate", action="store_true",
                    help="overlay>0 ゲートを無効化（payout フィルタのみ）")
    ap.add_argument("--ev-threshold", type=float, default=None,
                    help="複勝 EV 閾値の上書き（既定は bet_threshold_map の複勝値）")
    ap.add_argument("--takeout", type=float, default=0.2, help="連系推定オッズの控除率（複勝は payout 実測で決済）")
    ap.add_argument("--model-version", default=None, help="モデル版（部分一致）。未指定なら最新")
    ap.add_argument("--unit", type=int, default=1, help="1点あたりの単位（既定1）")
    return run(ap.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
