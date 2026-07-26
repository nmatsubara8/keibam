"""全8券種 ROI × 買値範囲(オッズ帯別) テスト — JRDB順序edgeが控除を超える帯があるか。

これまで単勝(帰無)・三連単(帰無)中心。だが券種で控除が違い（複勝/単勝20% / 枠連/馬連/
ワイド22.5% / 馬単/三連複25% / 三連単27.5%）、かつ卍氏の**買値範囲**＝価値は特定オッズ帯に
偏る、という2点を本スクリプトで同時に検証する。

8券種:
  単勝 tansho     : 帰無の基準（P1着＝市場効率）。参考掲載。
  複勝 fukusho    : 単一馬・控除20%（最低）・top3内。place強度が最も直接効く候補。
  枠連 wakuren    : 枠(1-8)ペア。馬番→枠番(KYI@324)で集約。低分散。
  馬連/ワイド/馬単/三連複/三連単: 既存。

各券種で 市場≈Harville(単勝由来), モデル=JRDB place強度(goal_juni/ichi_idx)。
EV=model/market×(1−takeout[券種])。topk/ev 戦略＋placebo＋実配当決済＋券種別控除。

買値範囲(卍): 賭けの「フェアオッズ=1/市場的中率」でオッズ帯に分け、帯別ROIを出す。さらに
**train で最良帯を選び→test で同じ帯だけ賭ける**（帯選択は train のみ＝OOS・事後掘り無し。
ただし K帯からの選択は多重比較。placebo と比較して判定）。

要データ: JRDB(KYI/SED)＋return_tables。実行:
  python multibet_roi_test.py --jrdb-dir data/jrdb_txt --strategy ev
"""
from __future__ import annotations

import argparse
from itertools import combinations, permutations

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.constants._takeout import TAKEOUT
from src.policies._harville import (
    combo_probability,
    combo_probability_place_strength,
    prob_place_place_strength,
    prob_quinella,
    prob_quinella_place_strength,
)
from src.tuning._payoffs import multi_bet_payoff_lookup, place_payoff_lookup_from_returns
from trifecta_jrdb_test import _place_probs, fit_coef, load_races

# 検証対象8券種（単勝は帰無基準として参考掲載）
BET_TYPES = [BetType.FUKUSHO, BetType.WAKUREN, BetType.WIDE, BetType.UMAREN,
             BetType.UMATAN, BetType.SANRENPUKU, BetType.SANRENTAN]
_UNORDERED = {BetType.UMAREN, BetType.WIDE, BetType.SANRENPUKU}
_SIZE = {BetType.WIDE: 2, BetType.UMAREN: 2, BetType.UMATAN: 2,
         BetType.SANRENPUKU: 3, BetType.SANRENTAN: 3}
_NAME = {BetType.TANSHO: "単勝", BetType.FUKUSHO: "複勝", BetType.WAKUREN: "枠連",
         BetType.WIDE: "ワイド", BetType.UMAREN: "馬連", BetType.UMATAN: "馬単",
         BetType.SANRENPUKU: "三連複", BetType.SANRENTAN: "三連単"}
# フェアオッズ(1/的中率)による買値帯。上端は open。卍の買値範囲の器。
ODDS_BANDS = [(1.0, 5.0), (5.0, 15.0), (15.0, 50.0), (50.0, 200.0), (200.0, 1e18)]
_BAND_LABEL = ["<5", "5-15", "15-50", "50-200", "200+"]


def _band(fair_odds: float) -> int:
    for i, (lo, hi) in enumerate(ODDS_BANDS):
        if lo <= fair_odds < hi:
            return i
    return len(ODDS_BANDS) - 1


def _n_places(n_runners: int) -> int:
    """複勝の的中枠数（JRA規程: 8頭以上=3・5-7頭=2・4頭以下=1）。"""
    if n_runners >= 8:
        return 3
    if n_runners >= 5:
        return 2
    return 1


def _bracket_map(waku: dict) -> dict:
    """枠番→[馬番...]。"""
    out: dict = {}
    for uma, wk in waku.items():
        out.setdefault(int(wk), []).append(int(uma))
    return out


def _wakuren_prob(q, probs, brackets, pair) -> float:
    """枠連ペア{A,B}の的中率＝Σ 該当馬ペアの馬連(place強度)確率（互いに排反な top2 事象の和）。"""
    a, b = pair
    tot = 0.0
    if a == b:
        for i, j in combinations(brackets.get(a, []), 2):
            tot += prob_quinella_place_strength(q, probs, i, j)
    else:
        for i in brackets.get(a, []):
            for j in brackets.get(b, []):
                tot += prob_quinella_place_strength(q, probs, i, j)
    return tot


def _wakuren_market_prob(q, brackets, pair) -> float:
    a, b = pair
    tot = 0.0
    if a == b:
        for i, j in combinations(brackets.get(a, []), 2):
            tot += prob_quinella(q, i, j)
    else:
        for i in brackets.get(a, []):
            for j in brackets.get(b, []):
                tot += prob_quinella(q, i, j)
    return tot


def _candidates(q, bet_type, top_m):
    """券種別に候補(combo, market_prob, model_prob 用の identity)を生成。

    返り値: [(combo_key_for_settlement, combo_for_prob, is_bracket)]。
    """
    top = [u for u, _ in sorted(q.items(), key=lambda kv: -kv[1])[:top_m]]
    sz = _SIZE[bet_type]
    gen = combinations if bet_type in _UNORDERED else permutations
    return [tuple(c) for c in gen(top, sz)]


def _scored_bets(r, coef, bet_type, *, strategy, top_m, ev_threshold, placebo, rng):
    """1レースの (combo_key, fair_odds, score) 列を返す（券種横断の共通入口）。"""
    q = r["q"]
    sig = r["sig"]
    if placebo and coef:
        keys = list(sig)
        vals = [sig[u] for u in keys]
        rng.shuffle(vals)
        sig = dict(zip(keys, vals, strict=False))
    plc = _place_probs(q, sig, coef) if coef else q
    takeout = TAKEOUT.get(bet_type, 0.2)
    out = []  # (key, fair_odds, score)

    if bet_type == BetType.FUKUSHO:
        npl = _n_places(len(q))
        top = [u for u, _ in sorted(q.items(), key=lambda kv: -kv[1])[:top_m]]
        for h in top:
            mp = prob_place_place_strength(q, q, h, npl)
            if mp <= 0:
                continue
            pp = prob_place_place_strength(q, plc, h, npl) if coef else mp
            fo = 1.0 / mp
            score = (pp / mp * (1 - takeout)) if strategy == "ev" else pp
            if strategy == "ev" and score <= ev_threshold:
                continue
            out.append(((h,), fo, score))
        return out

    if bet_type == BetType.WAKUREN:
        brackets = _bracket_map(r.get("waku", {}))
        if len(brackets) < 2:
            return out
        top_b = sorted(brackets, key=lambda B: -sum(q.get(u, 0.0) for u in brackets[B]))[:top_m]
        for a, b in combinations(top_b, 2):
            pair = (a, b)
            mp = _wakuren_market_prob(q, brackets, pair)
            if mp <= 0:
                continue
            pp = _wakuren_prob(q, plc, brackets, pair) if coef else mp
            fo = 1.0 / mp
            score = (pp / mp * (1 - takeout)) if strategy == "ev" else pp
            if strategy == "ev" and score <= ev_threshold:
                continue
            out.append((tuple(sorted(pair)), fo, score))
        # 同枠(ゾロ)候補: 2頭以上の枠
        for a in top_b:
            if len(brackets.get(a, [])) < 2:
                continue
            mp = _wakuren_market_prob(q, brackets, (a, a))
            if mp <= 0:
                continue
            pp = _wakuren_prob(q, plc, brackets, (a, a)) if coef else mp
            score = (pp / mp * (1 - takeout)) if strategy == "ev" else pp
            if strategy == "ev" and score <= ev_threshold:
                continue
            out.append(((a, a), 1.0 / mp, score))
        return out

    # 連系（馬連/ワイド/馬単/三連複/三連単）
    for combo in _candidates(q, bet_type, top_m):
        mp = combo_probability(bet_type, q, combo)
        if mp <= 0:
            continue
        pp = combo_probability_place_strength(bet_type, q, plc, combo) if coef else mp
        fo = 1.0 / mp
        score = (pp / mp * (1 - takeout)) if strategy == "ev" else pp
        if strategy == "ev" and score <= ev_threshold:
            continue
        key = tuple(sorted(combo)) if bet_type in _UNORDERED else tuple(combo)
        out.append((key, fo, score))
    return out


def backtest(races, coef, payoffs, bet_type, *, strategy, top_m, max_bets,
             ev_threshold=1.0, placebo=False, band_filter=None, seed=0):
    """全体ROIと帯別ROIを返す。band_filter を渡すとその帯のみ賭ける（買値範囲テスト）。"""
    rng = np.random.default_rng(seed)
    stake = ret = 0.0
    n_bets = hit = 0
    band_stake = np.zeros(len(ODDS_BANDS))
    band_ret = np.zeros(len(ODDS_BANDS))
    band_hit = np.zeros(len(ODDS_BANDS), dtype=int)
    ledger: dict = {}   # rid -> [stake_sum, ret_sum]（レース単位ブロックbootstrap用）
    for r in races:
        rid = r["rid"]
        wins = payoffs.get(rid)
        if not wins:
            continue
        # 決済 win_map: {combo_key: payoff/100}
        if bet_type == BetType.FUKUSHO:
            win_map = {(k[1],): v / 100.0 for k, v in wins.items()}  # wins は {(rid,uma):pay}
        else:
            win_map = {c: p / 100.0 for c, p in wins}
        bets = _scored_bets(r, coef, bet_type, strategy=strategy, top_m=top_m,
                            ev_threshold=ev_threshold, placebo=placebo, rng=rng)
        bets.sort(key=lambda t: -t[2])
        for key, fo, _score in bets[:max_bets]:
            bd = _band(fo)
            if band_filter is not None and bd != band_filter:
                continue
            n_bets += 1
            stake += 1.0
            band_stake[bd] += 1.0
            payout = win_map.get(key, 0.0)
            ret += payout
            band_ret[bd] += payout
            if payout > 0:
                hit += 1
                band_hit[bd] += 1
            acc = ledger.setdefault(rid, [0.0, 0.0])
            acc[0] += 1.0
            acc[1] += payout
    band_roi = [band_ret[i] / band_stake[i] if band_stake[i] else float("nan")
                for i in range(len(ODDS_BANDS))]
    return {"roi": ret / stake if stake else 0.0, "n_bets": n_bets, "hit": hit,
            "takeout": TAKEOUT.get(bet_type, 0.2),
            "band_roi": band_roi, "band_stake": band_stake.tolist(),
            "band_hit": band_hit.tolist(),
            "ledger": [(v[0], v[1]) for v in ledger.values()]}


def bootstrap_roi_ci(ledger, n_boot=2000, seed=0):
    """レース単位ブロック・ブートストラップで ROI の (2.5%, 50%, 97.5%) を返す。

    レース内の賭けは相関する（同一結果・重複買い目）ため、賭け単位ではなくレース単位で
    (stake_sum, ret_sum) を再標本化する＝相関を壊さない honest な分散。的中が数本しか
    無い穴セルでは下限がほぼ 0 に落ち、点推定の 1.0 超えが偶然だと露見する。
    """
    if not ledger:
        return (float("nan"), float("nan"), float("nan"))
    arr = np.asarray(ledger, dtype=float)          # (n_races, 2) = [stake, ret]
    stakes, rets = arr[:, 0], arr[:, 1]
    n = len(arr)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(n_boot, n))
    bs = rets[idx].sum(axis=1)
    st = stakes[idx].sum(axis=1)
    roi = np.where(st > 0, bs / st, 0.0)
    return (float(np.percentile(roi, 2.5)), float(np.percentile(roi, 50)),
            float(np.percentile(roi, 97.5)))


def _fukusho_payoffs(rt) -> dict:
    """複勝を {rid: {(rid,uma): pay}} 形に（backtest の払戻取り出しに合わせる）。"""
    flat = place_payoff_lookup_from_returns(rt)   # {(rid,uma): pay}
    out: dict = {}
    for (rid, uma), pay in flat.items():
        out.setdefault(str(rid), {})[(str(rid), int(uma))] = pay
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--returns", default=LocalPaths.RAW_RETURN_TABLES_PATH)
    ap.add_argument("--with-tyb", action="store_true")
    ap.add_argument("--strategy", choices=("ev", "topk"), default="ev")
    ap.add_argument("--top-m", type=int, default=6)
    ap.add_argument("--max-bets", type=int, default=3)
    ap.add_argument("--train-frac", type=float, default=0.6)
    args = ap.parse_args()

    import os
    if not os.path.exists(args.returns):
        raise SystemExit(f"return_tables がありません: {args.returns}")
    rt = pd.read_pickle(args.returns)
    races, signals = load_races(args.jrdb_dir, with_tyb=args.with_tyb)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    coef = fit_coef(tr, signals)
    print(f"レース {len(races):,}（train {len(tr):,} / test {len(te):,}）signals={signals}")
    print(f"係数(train三連単NLL最小): { {k: round(v, 2) for k, v in coef.items()} }")

    kw = dict(strategy=args.strategy, top_m=args.top_m, max_bets=args.max_bets)
    print(f"\n【8券種 ROI（控除別・{args.strategy}戦略）／ROIはレース単位ブロックbootstrap 95%CI付き】")
    print(f"{'券種':<8}{'控除':>6}{'JRDB込み':>10}{'CI下限':>9}{'CI上限':>9}{'placebo':>9}{'的中':>6}{'点数':>7}  判定")
    print("-" * 78)
    results = {}
    for bt in BET_TYPES:
        if bt == BetType.FUKUSHO:
            pay = _fukusho_payoffs(rt)
        else:
            pay = multi_bet_payoff_lookup(rt, bt)
        base = backtest(te, None, pay, bt, **kw)
        jr = backtest(te, coef, pay, bt, **kw)
        pl = backtest(te, coef, pay, bt, placebo=True, **kw)
        results[bt] = (pay, base, jr, pl)
        lo, _mid, hi = bootstrap_roi_ci(jr["ledger"])
        # 真の edge 判定は「点推定>1」ではなく「CI下限>1.0 かつ placebo を上回る」
        edge = lo > 1.0 and jr["roi"] > pl["roi"] + 0.03
        print(f"{_NAME[bt]:<8}{jr['takeout']*100:>5.1f}%{jr['roi']:>10.3f}{lo:>9.3f}{hi:>9.3f}"
              f"{pl['roi']:>9.3f}{jr['hit']:>6}{jr['n_bets']:>7}  "
              + ("★控除超え" if edge else "帰無"))
    print("  ※判定は「点推定>1」ではなく【CI下限>1.0】が必須。的中数一桁のセルは下限がほぼ0＝ノイズ。")

    # ── 買値範囲（オッズ帯別ROI＋train定義帯のOOS検証）──
    print(f"\n【買値範囲: フェアオッズ帯別ROI（JRDB込み・test）】 帯={_BAND_LABEL}")
    print(f"{'券種':<8}" + "".join(f"{lbl:>9}" for lbl in _BAND_LABEL))
    print("-" * 70)
    for bt in BET_TYPES:
        _, _, jr, _ = results[bt]
        cells = []
        for i in range(len(ODDS_BANDS)):
            roi = jr["band_roi"][i]
            n = int(jr["band_stake"][i])
            cells.append(f"{roi:>6.2f}({n})" if n and not np.isnan(roi) else f"{'—':>9}")
        print(f"{_NAME[bt]:<8}" + "".join(f"{c:>9}" for c in cells))

    print("\n【train定義買値帯を test で検証（帯選択=trainのみ＝OOS・CI下限で判定）】")
    print(f"{'券種':<8}{'選択帯':>7}{'test ROI':>10}{'CI下限':>9}{'CI上限':>9}{'placebo':>9}{'的中':>6}{'点数':>7}  判定")
    print("-" * 78)
    for bt in BET_TYPES:
        pay = results[bt][0]
        tr_full = backtest(tr, coef, pay, bt, **kw)
        # train で点数≥30 かつ ROI 最大の帯を選ぶ
        cand = [(tr_full["band_roi"][i], i) for i in range(len(ODDS_BANDS))
                if tr_full["band_stake"][i] >= 30 and not np.isnan(tr_full["band_roi"][i])]
        if not cand:
            print(f"{_NAME[bt]:<8}{'—':>7}{'(train賭け不足)':>32}")
            continue
        best_band = max(cand)[1]
        te_b = backtest(te, coef, pay, bt, band_filter=best_band, **kw)
        pl_b = backtest(te, coef, pay, bt, band_filter=best_band, placebo=True, **kw)
        lo, _mid, hi = bootstrap_roi_ci(te_b["ledger"])
        win = lo > 1.0 and te_b["roi"] > pl_b["roi"] + 0.03
        print(f"{_NAME[bt]:<8}{_BAND_LABEL[best_band]:>7}{te_b['roi']:>10.3f}{lo:>9.3f}{hi:>9.3f}"
              f"{pl_b['roi']:>9.3f}{te_b['hit']:>6}{te_b['n_bets']:>7}  "
              + ("★控除超え" if win else "帰無"))

    print("\n※ 真の edge は【CI下限>1.0 かつ placebo を明確に上回る】セルのみ。点推定の 1.0 超えは")
    print("  的中数一桁×高オッズ帯のファットテール・ノイズであり、CI下限で即棄却される（多重比較込み）。")
    print("  --strategy topk / --with-tyb / --top-m --max-bets も試す。")


if __name__ == "__main__":
    main()
