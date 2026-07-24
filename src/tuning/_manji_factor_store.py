"""①.5a ファクター事前計算層 — 卍補正の共有土台（全シナリオで使い回す）。

設計意図（効率性の要）:
- ①（featured）は **read-only**。ここでは一切変更しない。
- 卍因子の**バケットラベルは決定的**なので、featured から **1 回だけ** materialize して
  ``data/manji/factor_table.pkl`` に保存する。シナリオ（補正の仮説セット）が変わっても
  この表は再計算しない——シナリオは「どの因子を採るか＋重み」を選ぶだけで、補正列は
  この表と事後分布ストア（Step 2）の線形結合で安価に作れる。
- 因子には 2 系統がある:
    * 単一行で決まる因子（性・馬齢・枠・馬体重帯 …）＝ _manji_factors のベクトル化バケット。
    * **馬の履歴**を要する因子（近走フォーム/回収・通算勝率/回収）＝ ここで forward-only
      （＝当該走を**含めない** shift(1)）で数値列 ``mf_*`` を作り、_manji_factors 側の
      薄い帯化関数（f_recent*/f_career*）がそれを帯にする。

前進安全（リーク防止）:
- 履歴依拠の数値列は必ず「その馬の過去走のみ」で計算する（shift(1) で当該走を除外）。
  デビュー戦・履歴不足は NaN → 帯化で na（中立）になる。
- featured の index は race_id で**非ユニーク**（1 レース複数行＝各馬 1 行）。したがって
  キーは (race_id, 馬番)。本モジュールは行の位置対応を保って算出し、出力にも
  race_id/馬番/horse_id/date を持たせて下流が (race_id,馬番) で結合できるようにする。

実行:
  python -m src.tuning._manji_factor_store            # featured から生成し保存（既存はスキップ）
  python -m src.tuning._manji_factor_store --force    # 既存を無視して再生成
"""
from __future__ import annotations

import logging
import os

import numpy as np
import pandas as pd

from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols

logger = logging.getLogger(__name__)

# _manji_factor_store が付与する履歴依拠の数値列（_manji_factors の f_recent*/f_career* が帯化）。
RECENT_WINDOWS = (3, 5)
DETAIL_WINDOW = 5  # 近走詳細（出遅れ/不利/着差/トラック・馬場別）の集約窓＝過去5走


def _resolve_num_col(featured: pd.DataFrame, *names):
    """候補列名の最初に在るものを数値化して返す（無ければ None）。"""
    for nm in names:
        if nm in featured.columns:
            return pd.to_numeric(featured[nm], errors="coerce").to_numpy()
    return None


MF_COLS = tuple(
    [f"mf_recent{n}_avg_rank" for n in RECENT_WINDOWS]
    + [f"mf_recent{n}_winrate" for n in RECENT_WINDOWS]
    + [f"mf_recent{n}_recovery" for n in RECENT_WINDOWS]
    + ["mf_career_n", "mf_career_winrate", "mf_career_recovery",
       "mf_prev_rank", "mf_interval"]  # 前走着順・レース間隔（履歴から算出＝元列不要）
)


def build_recent_form_features(featured: pd.DataFrame) -> pd.DataFrame:
    """featured から馬×時刻の履歴依拠 数値列（mf_*）を forward-only で作る。

    Returns
    -------
    index=featured.index（race_id, 非ユニーク・行順を保存）で MF_COLS を持つ DataFrame。
    当該走は含めない（shift(1)）。履歴不足は NaN（下流の帯化で na）。
    """
    n = len(featured)
    horse = featured["horse_id"].astype(str) if "horse_id" in featured.columns \
        else pd.Series(np.arange(n).astype(str), index=featured.index)
    work = pd.DataFrame(
        {
            "pos": np.arange(n),  # 元の行位置（ソート後に戻すため）
            "horse_id": horse.to_numpy(),
            "date": pd.to_datetime(featured["date"], errors="coerce").to_numpy()
            if "date" in featured.columns else np.arange(n),
            "race_id": featured.index.astype(str).to_numpy(),
            "rank": pd.to_numeric(featured[ResultsCols.RANK], errors="coerce").to_numpy(),
            "odds": pd.to_numeric(featured[ResultsCols.TANSHO_ODDS], errors="coerce").to_numpy(),
        }
    )
    work["win"] = (work["rank"] == 1).astype(float)
    work["ret"] = work["odds"] * work["win"]  # 単勝フラット回収（着1なら odds、他0）

    # --- 近走の詳細シグナル（元列がある場合のみ。無ければ列を作らず=下流で na） ---------
    # (out_name, work列名, 集約) を added に積み、後で shift+rolling(DETAIL_WINDOW) する。
    added: list[tuple[str, str, str]] = []
    from src.policies._manji_factors import _onehot_cat, _race_type_series
    rk = work["rank"].to_numpy()

    deo = _resolve_num_col(featured, "出遅れ", "deokure", "出遅", "prev_deokure_raw")
    if deo is not None:
        work["_deokure"] = deo
        added.append(("mf_recent_deokure", "_deokure", "max"))     # 近走に出遅れ有=1
    tro = _resolve_num_col(featured, "不利", "trouble", "道中不利", "prev_trouble_raw")
    if tro is not None:
        work["_trouble"] = tro
        added.append(("mf_recent_trouble", "_trouble", "max"))     # 近走に不利有=1
    tsec = _resolve_num_col(featured, "time_seconds")
    if tsec is not None:  # 勝ち馬(レース内最小タイム)からの差[秒]
        rid = featured.index.astype(str).to_numpy()
        wmin = pd.DataFrame({"r": rid, "t": tsec}).groupby("r")["t"].transform("min").to_numpy()
        work["_margin"] = tsec - wmin
        added.append(("mf_recent_close", "_margin", "min"))        # 近走の最小着差[秒]
    rt = _race_type_series(featured)
    if rt is not None:  # 過去走のトラック別 最高着順
        surf = rt.astype(str).to_numpy()
        work["_dirt_rank"] = np.where(surf == "ダート", rk, np.nan)
        work["_turf_rank"] = np.where(surf == "芝", rk, np.nan)
        added.append(("mf_recent_dirt_bestrank", "_dirt_rank", "min"))
        added.append(("mf_recent_turf_bestrank", "_turf_rank", "min"))
    gr = _onehot_cat(featured, "ground_state1__")
    if gr is None and "ground_state" in featured.columns:
        gr = featured["ground_state"].astype(str)
    if gr is not None:  # 過去走の道悪(重/不良) 最高着順
        grd = pd.Series(gr).astype(str).to_numpy()
        work["_heavy_rank"] = np.where(np.isin(grd, ["重", "不良"]), rk, np.nan)
        added.append(("mf_recent_heavy_bestrank", "_heavy_rank", "min"))
    # 距離（course_len）: 前走との差 mf_dist_change 用。元列があれば。
    clen = _resolve_num_col(featured, "course_len", "距離", "course_len_m", "distance")
    has_courselen = clen is not None
    if has_courselen:
        work["_courselen"] = clen

    # 馬ごとに日付順（同日は race_id で安定化）。当該走を除くため shift(1)。
    w = work.sort_values(["horse_id", "date", "race_id"], kind="stable")
    grp = w.groupby("horse_id", sort=False)
    w["_past_rank"] = grp["rank"].shift(1)
    w["_past_win"] = grp["win"].shift(1)
    w["_past_ret"] = grp["ret"].shift(1)

    grp2 = w.groupby("horse_id", sort=False)
    for nwin in RECENT_WINDOWS:
        w[f"mf_recent{nwin}_avg_rank"] = grp2["_past_rank"].transform(
            lambda s, k=nwin: s.rolling(k, min_periods=1).mean())
        w[f"mf_recent{nwin}_winrate"] = grp2["_past_win"].transform(
            lambda s, k=nwin: s.rolling(k, min_periods=1).mean())
        w[f"mf_recent{nwin}_recovery"] = grp2["_past_ret"].transform(
            lambda s, k=nwin: s.rolling(k, min_periods=1).mean())
    # 通算（前走まで・expanding）。全過去依拠ファクターの土台。
    w["mf_career_n"] = grp2["_past_win"].transform(lambda s: s.expanding(min_periods=1).count())
    w["mf_career_winrate"] = grp2["_past_win"].transform(lambda s: s.expanding(min_periods=1).mean())
    w["mf_career_recovery"] = grp2["_past_ret"].transform(lambda s: s.expanding(min_periods=1).mean())

    # 前走着順（prev_finish 用）・レース間隔[日]（rotation/age_rotation 用）を履歴から算出。
    w["mf_prev_rank"] = w["_past_rank"]  # 直前走の着順
    past_date = grp2["date"].shift(1)
    w["mf_interval"] = (w["date"] - past_date) / np.timedelta64(1, "D")
    extra_cols: list[str] = []
    if has_courselen:  # 距離変更[m or 100m単位]（dist_change/dist_age 用）
        w["mf_dist_change"] = w["_courselen"] - grp2["_courselen"].shift(1)
        extra_cols.append("mf_dist_change")

    # 近走詳細: 過去 DETAIL_WINDOW 走の集約（当該走除外の shift 後にローリング）。
    for out_name, src, agg in added:
        w["_pastd"] = grp2[src].shift(1)
        w[out_name] = w.groupby("horse_id", sort=False)["_pastd"].transform(
            lambda s, a=agg: getattr(s.rolling(DETAIL_WINDOW, min_periods=1), a)())

    # 元の行順（pos）に戻して featured.index に貼り直す（race_id 非ユニークでも位置一致）。
    w = w.sort_values("pos", kind="stable")
    computed = list(MF_COLS) + extra_cols + [o for o, _, _ in added]
    out = pd.DataFrame({c: w[c].to_numpy() for c in computed}, index=featured.index)
    return out


def compute_head2head(featured: pd.DataFrame, *, lookback: int = 10) -> pd.Series:
    """同一レース対戦履歴の純スコア（forward-only）。

    今走の各馬について、**過去に同じレースで対戦した今走の他出走馬**との着順比較を集計。
    卍ルール: 過去に相手より着順が良かった側は −1（既に優位を証明＝割引/過大評価）、
    悪かった側は +1（過小評価＝妙味）。net = Σ(+1: 過去に負け / −1: 過去に勝ち)。
    過去対戦が1件も無ければ NaN（下流の帯化で na）。lookback=各馬の直近何走まで遡るか。

    計算量は「レース数 × 出走頭数 × lookback × 頭数」で重い。既定 OFF（build_factor_table の
    with_h2h=True で有効化）＋ factor_table キャッシュ運用を推奨。
    """
    from collections import defaultdict

    rid = featured.index.astype(str).to_numpy()
    horse = (featured["horse_id"].astype(str).to_numpy()
             if "horse_id" in featured.columns else rid)
    date = pd.to_datetime(featured["date"], errors="coerce").to_numpy() \
        if "date" in featured.columns else np.arange(len(featured))
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce").to_numpy()
    n = len(featured)

    race_rows: dict[str, list[int]] = defaultdict(list)
    for i in range(n):
        race_rows[rid[i]].append(i)
    # 馬 → 発走日順の (date, race_id, rank)
    horse_hist: dict[str, list] = defaultdict(list)
    for i in np.argsort(date, kind="stable"):
        horse_hist[horse[i]].append((date[i], rid[i], rank[i]))
    # race_id → {horse: rank}
    race_ranks = {r: {horse[i]: rank[i] for i in rows} for r, rows in race_rows.items()}

    score = np.full(n, np.nan)
    for r, rows in race_rows.items():
        d = date[rows[0]]
        S = {horse[i] for i in rows}
        for i in rows:
            hi = horse[i]
            past = [x for x in horse_hist[hi] if x[0] < d][-lookback:]
            net = 0
            cmp = 0
            for _pd, prid, prank in past:
                if np.isnan(prank):
                    continue
                for hj, rj in race_ranks.get(prid, {}).items():
                    if hj in S and hj != hi and not np.isnan(rj):
                        if prank < rj:
                            net -= 1  # 過去に勝ち → 割引
                            cmp += 1
                        elif prank > rj:
                            net += 1  # 過去に負け → 妙味
                            cmp += 1
            if cmp > 0:
                score[i] = net
    return pd.Series(score, index=featured.index)


def build_factor_table(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    with_h2h: bool = False,
    h2h_lookback: int = 10,
) -> pd.DataFrame:
    """featured（①）から因子バケット表を1回だけ生成する（全シナリオ共有）。

    - 履歴依拠の mf_* を forward-only で付けた上で、_manji_factors.buckets() を全因子に適用。
    - 出力は (race_id, 馬番) をキー列に持ち、horse_id/date（前進スライス用）＋各因子の
      バケットラベル列を持つ。featured は変更しない。

    Returns
    -------
    columns = ["race_id", "馬番", "horse_id", "date", <各因子のバケット列...>]
    """
    from src.policies._manji_factors import FACTORS, buckets

    factor_names = factor_names or list(FACTORS)
    # ① を壊さずに mf_* を付けたビューを作る。index=race_id は非ユニークなので
    # join（ラベル結合＝重複でカルテシアン展開）ではなく **位置代入**（numpy）で足す。
    mf = build_recent_form_features(featured)
    view = featured.copy()
    for c in mf.columns:  # 近走詳細の追加列も含めて全 mf_* を付与
        view[c] = mf[c].to_numpy()  # 位置代入（index 整合を回避）
    if with_h2h:  # 同一レース対戦履歴（重い。有効時のみ）
        view["mf_h2h_score"] = compute_head2head(featured, lookback=h2h_lookback).to_numpy()

    bk = buckets(view, factor_names)  # index=race_id（非ユニーク）, 各因子のバケット列
    # 出力も位置ベースで組む（RangeIndex）。キーは (race_id, 馬番)。
    data = {
        "race_id": featured.index.astype(str).to_numpy(),
        "馬番": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").astype("Int64").to_numpy(),
        "horse_id": (featured["horse_id"].astype(str).to_numpy()
                     if "horse_id" in featured.columns else np.full(len(featured), pd.NA)),
        "date": (pd.to_datetime(featured["date"], errors="coerce").to_numpy()
                 if "date" in featured.columns else np.full(len(featured), pd.NaT)),
    }
    for c in bk.columns:
        data[c] = bk[c].to_numpy()
    return pd.DataFrame(data)


def save_factor_table(table: pd.DataFrame, path: str = LocalPaths.MANJI_FACTOR_TABLE_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    table.to_pickle(path)


def load_factor_table(path: str = LocalPaths.MANJI_FACTOR_TABLE_PATH) -> pd.DataFrame | None:
    """事前計算済み factor_table を読む（無ければ None）。"""
    if not os.path.exists(path):
        return None
    return pd.read_pickle(path)


def _coverage_report(table: pd.DataFrame, factor_names: list[str]) -> None:
    """各因子の非 na 率を出す（履歴依拠は序盤 na が多いのが正常）。"""
    from src.policies._manji_factors import NA
    n = len(table)
    logger.info("[factor-store] %d 行 / %d 因子", n, len(factor_names))
    for f in factor_names:
        if f not in table.columns:
            continue
        nonna = int((table[f].astype(object) != NA).sum())
        logger.info("  %-18s 非na %6.1f%% (%d)", f, 100.0 * nonna / max(n, 1), nonna)


def main() -> None:
    import argparse

    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="卍①.5a ファクター事前計算表を生成（全データでキャッシュ）")
    ap.add_argument("--force", action="store_true", help="既存 factor_table を無視して再生成")
    ap.add_argument("--coverage", action="store_true", help="因子ごとの非na率を表示")
    ap.add_argument("--with-h2h", action="store_true",
                    help="同一レース対戦履歴(head2head)も含める（重い。1回キャッシュ生成向け）")
    ap.add_argument("--h2h-lookback", type=int, default=10, help="対戦を遡る各馬の直近走数")
    args = ap.parse_args()

    path = LocalPaths.MANJI_FACTOR_TABLE_PATH
    if os.path.exists(path) and not args.force:
        print(f"既に存在します: {path}（再生成は --force）")
        return

    import time

    from app._model_eval import load_featured_data
    from src.policies._manji_factors import FACTORS

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（先に rebuild-featured を実行してください）")
        return

    print(f"[factor-store] featured {len(featured):,} 行から因子表を生成します"
          f"（head2head={'含む' if args.with_h2h else '除外'}）...", flush=True)
    if args.with_h2h:
        print("  ※ head2head は全レースの対戦履歴を走査するため時間がかかります", flush=True)
    t0 = time.time()
    factor_names = list(FACTORS)
    table = build_factor_table(featured, factor_names,
                               with_h2h=args.with_h2h, h2h_lookback=args.h2h_lookback)
    save_factor_table(table, path)
    if args.coverage:
        _coverage_report(table, factor_names)
    print(f"\n生成完了: {path}（{len(table):,} 行 / {len(factor_names)} 因子 / "
          f"{time.time() - t0:.0f}s）", flush=True)
    print("以後 prepare_shared / manji_scenario_select はこのキャッシュを load_factor_table で再利用します。")
    print("注意: これは**全データ**の表です。--limit で部分実行してもキー(race_id,馬番)で正しく整列します。")


if __name__ == "__main__":
    main()
