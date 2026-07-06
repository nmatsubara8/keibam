"""seed の複勝 EV バックテスト（正当版）— odds.csv の実払戻で決済。

look-ahead でない正当な複勝検証: Place ヘッドが **事前情報のみ**（モデル確率 + 単勝由来の
推定複勝オッズ）で EV 選定し、odds.csv の **実複勝払戻**で決済する。選定=model、決済=payout
なので未来のぞき見はない（overlay 検証とは別物）。

前提:
  - data/raw/seed_featured_data.pkl（build_seed_featured.py）
  - 学習済み seed モデル（例 seed35y_ho、2020-21 を holdout）
  - odds.csv（複勝/単勝の払戻表）

これは src.simulation._backtest の select_candidates/settle_candidates をそのまま使い、
払戻テーブルだけ seed 用（odds.csv 由来）に差し替えた薄いラッパ。ROI が控除率20%(=ROI 80%)を
複数年安定して超え、的中≥30 なら複勝にエッジ。単勝35年効率＋複勝478行効率から、80%前後で
市場効率再確認の公算が高い。

使い方:
    python verify_seed_fukusho_backtest.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_odds.csv" \
        --version seed35y_ho --years 2020 2021
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

C_RACE_ID = "レースID"


def _read_csv(path: str):
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("odds.csv 読込失敗。")


def _single_table(odds: pd.DataFrame, prefix: str, slots: int) -> pd.DataFrame:
    """単勝/複勝の payout 表を {race_id: win_k/return_k} DataFrame に変換。

    win_k=的中馬番, return_k=払戻(円/100)。欠損スロットは win=0(=エントリ無し)。
    """
    recs: dict = {}
    for _, r in odds.iterrows():
        rid = str(int(r[C_RACE_ID]))
        row = {}
        for k in range(slots):
            mcol, ocol = f"{prefix}{k+1}_馬番", f"{prefix}{k+1}_オッズ"
            uma = r.get(mcol)
            od = r.get(ocol)
            # win は **文字列**（実 ReturnProcessor と同形式）。int にすると行 .loc で float 昇格し
            # str(1.0)="1.0"→int() 例外になる。_match は str→int で照合、"0"=エントリ無し。
            row[f"win_{k}"] = str(int(uma)) if pd.notna(uma) else "0"
            row[f"return_{k}"] = float(od) if pd.notna(od) else 0.0
        recs[rid] = row
    df = pd.DataFrame.from_dict(recs, orient="index")
    return df


class _SeedReturnProcessor:
    """settle_candidates 用の払戻テーブル供給（odds.csv 由来）。全8券種キーを持つ。"""

    def __init__(self, odds: pd.DataFrame) -> None:
        from src.constants._bet_types import BetType

        empty = pd.DataFrame()
        self.preprocessed_data = {
            BetType.TANSHO: _single_table(odds, "単勝", 2),
            BetType.FUKUSHO: _single_table(odds, "複勝", 5),
            BetType.WAKUREN: empty, BetType.UMAREN: empty, BetType.UMATAN: empty,
            BetType.WIDE: empty, BetType.SANRENPUKU: empty, BetType.SANRENTAN: empty,
        }


def _load_model(version: str | None):
    from app._data_loader import find_model_paths, load_model_from_path, load_win_head_for

    paths = find_model_paths("models")
    for p in paths:
        if version is None or version in os.path.basename(p):
            return load_model_from_path(p), load_win_head_for(p), p
    # 見つからない: 利用可能なモデルを表示して切り分ける
    print("[NG] モデルが見つからない。利用可能なモデル（models/*/*_keibam.pickle）:")
    for p in paths:
        print(f"    {p}  →  --version に含められる名前: {os.path.basename(p).replace('_keibam.pickle','')}")
    if not paths:
        print("    （0件。retrain の保存先を確認。models/ 直下に <version>/<version>_keibam.pickle があるか）")
    return None, None, None


def run(args) -> int:
    from src.constants._bet_types import BetType
    from src.simulation._backtest import (BetTypeStats, default_thresholds,
                                          select_candidates, settle_candidates)

    if not os.path.isfile(args.odds):
        print(f"[NG] odds.csv が無い: {args.odds}")
        return 2
    if not os.path.isfile(args.featured):
        print(f"[NG] seed featured が無い: {args.featured}（build_seed_featured.py）")
        return 2

    featured = pd.read_pickle(args.featured)
    place_ai, win_ai, mpath = _load_model(args.version)
    if place_ai is None:
        print("[NG] モデルが見つからない（models/）。--version を確認。")
        return 2
    print("=" * 80)
    print(f"seed 複勝 EV バックテスト  model={os.path.basename(mpath)}  "
          f"win_head={'有' if win_ai else '無'}")
    print("=" * 80)

    odds = _read_csv(args.odds)
    rp = _SeedReturnProcessor(odds)
    n_fuku = int((rp.preprocessed_data[BetType.FUKUSHO].get("win_0", pd.Series(dtype=int)) != 0).sum())
    print(f"払戻表: 複勝 {len(rp.preprocessed_data[BetType.FUKUSHO]):,} レース（win_0有 {n_fuku:,}）")

    ev_thr = args.ev_threshold if args.ev_threshold is not None else default_thresholds().get(BetType.FUKUSHO, 1.0)
    thresholds = {BetType.FUKUSHO: ev_thr}

    def _bt(X, label):
        cands = select_candidates(
            place_ai.effective_model, X,
            win_model=win_ai.effective_model if win_ai is not None else None,
            thresholds=thresholds, takeout=args.takeout,
        )
        cands = [c for c in cands if c.bet_type == BetType.FUKUSHO]
        per = settle_candidates(cands, rp, unit=1)
        s = per.get(BetType.FUKUSHO, BetTypeStats(BetType.FUKUSHO))
        rel = "✓" if s.n_hits >= 30 else "参考"
        print(f"  {label:<10} 点数={s.n_bets:>6,} 的中={s.hit_rate*100:5.1f}% "
              f"回収率={s.roi*100:6.1f}% 除外後={s.roi_ex_top*100:6.1f}% {rel}")
        return s

    rid = featured.index.astype(str)
    years = [str(y) for y in (args.years or [])]
    Xall = featured[rid.str[:4].isin(set(years))] if years else featured
    print(f"\n評価対象: {len(Xall):,} 行 / {Xall.index.nunique():,} レース "
          f"（EV閾値(複勝)={ev_thr}, takeout={args.takeout}）")
    print("-" * 80)
    _bt(Xall, "全体")
    if years:
        for y in years:
            _bt(featured[rid.str[:4] == y], y)

    print("\n" + "=" * 80)
    print("判定: 回収率>100% が複数年安定＋的中≥30(✓) で複勝にエッジ。80%前後なら控除率20%どおり=市場効率。")
    print("除外後回収率が大きく落ちる=フロック（万馬券依存）。選定=model・決済=実払戻で look-ahead 無し。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="seed 複勝 EV バックテスト（odds.csv 実払戻で決済）")
    ap.add_argument("odds", help="odds.csv（払戻表）")
    ap.add_argument("--featured", default="data/raw/seed_featured_data.pkl")
    ap.add_argument("--version", default=None, help="seed モデル版（部分一致・例 seed35y_ho）")
    ap.add_argument("--years", type=int, nargs="+", default=None, help="評価年（学習除外年＝OOS）")
    ap.add_argument("--ev-threshold", type=float, default=None, help="複勝 EV 閾値の上書き")
    ap.add_argument("--takeout", type=float, default=0.2, help="推定複勝オッズの控除率")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
