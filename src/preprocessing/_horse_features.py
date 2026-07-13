"""馬の過去成績（horse_results）から特徴量を生成する純粋関数群。

`DataMerger` の `_add_*` 集計を `(results, horse_results, ...) -> results` の自由関数として
切り出したもの（リーク無し: horse_results は当該レース日より前にカット済み前提）。
DataMerger からは薄い委譲メソッド経由で呼ぶ。特徴ファミリ単位でテスト可能。
"""

from __future__ import annotations

import pandas as pd

from src.constants._feature_cols import AGG_STATS, N_RACES_LIST, PACE_RECENT_N
from src.constants._horse_results_cols import HorseResultsCols as HRCols

GRADE_ORDINAL: dict = {
    "G1": 5, "Jpn1": 5, "G2": 4, "Jpn2": 4, "G3": 3, "Jpn3": 3, "L": 2, "OP": 1,
}


def filter_horse_results(horse_results: pd.DataFrame, n_races: int) -> pd.DataFrame:
    """直近 n_races レースに絞る（index=horse_id の前提）。"""
    return horse_results.sort_values("date", ascending=False).groupby(level=0).head(n_races)


def build_horse_results_from_results(results: pd.DataFrame) -> pd.DataFrame:
    """results（race_info マージ済み）から horse_results 相当フレームを再構成する（index=horse_id）。

    馬ページ(horse_results)を取得できていない馬の form を、既存の results 履歴だけから埋めるための
    アダプタ。着順/頭数/馬場/開催/course_len/race_type/斤量/騎手(=jockey_id)/date を
    HorseResultsCols 名で持つ（各行＝その馬の1過去走）。

    通過(CORNER) は results に有れば first_corner を導出する（アーカイブ取込 results は 1-4
    コーナー由来の通過を持つ）。これにより `add_pace_stats` が過去走から脚質(leg_type_binary)を
    計算でき、歴史馬(馬ページ未取得)にも展開×脚質が効かせられる。タイム(speed_figure)/賞金
    (PRIZE)/レース名(RACE_NAME) は無ければ付けない（列不在で各 add_* が自動スキップ）。

    リーク回避は呼び出し側の date スライス（date<target を searchsorted）に委ねる。ここでは
    純粋に「results の各行を過去走レコードへ写像」するだけ（着順という結果は過去走の事実であり、
    当該レースは date スライスで除外されるため漏れない）。
    """
    from src.constants._master import Master
    from src.constants._results_cols import ResultsCols

    if results is None or results.empty or "horse_id" not in results.columns:
        return pd.DataFrame()

    df = results.reset_index(drop=False) if results.index.name else results.copy()
    n = len(df)
    out = pd.DataFrame(index=range(n))
    out["horse_id"] = df["horse_id"].astype(str).str.replace(r"\.0$", "", regex=True).to_numpy()
    # date は datetime とは限らず、アーカイブ取込 results は "YYYY年MM月DD日" の文字列で来る。
    # 書式無し pd.to_datetime は日本語書式を読めず全 NaT→下段 dropna で全行消滅し、歴史馬の
    # horse_results が空になる（＝脚質 leg_type_binary が全馬同値 1.0 に潰れる根本原因）。書式を明示して救う。
    _d = df.get("date")
    if _d is None:
        out["date"] = pd.NaT
    else:
        _dt = pd.to_datetime(_d, errors="coerce", format="%Y年%m月%d日")
        if float(_dt.notna().mean()) < 0.5:          # 既に datetime / ISO 文字列 等
            _dt = pd.to_datetime(_d, errors="coerce")
        out["date"] = _dt.to_numpy()
    out[HRCols.RANK] = pd.to_numeric(df.get(ResultsCols.RANK), errors="coerce").to_numpy()
    # 頭数=実出走頭数。race_id があれば groupby サイズが常に正。n_horses 列は index 崩れ
    # (results の race_id が列で index が RangeIndex 等)で value_counts が全 1 に縮退している
    # 場合があり、それを頭数に使うと _pace_num=first_corner/頭数 が clip で 1.0 固着し脚質が
    # 全馬同値(1.0)に潰れる。よって race_id からの実頭数を優先し、無い時だけ n_horses に退避。
    if "race_id" in df.columns:
        out[HRCols.N_HORSES] = df.groupby("race_id")["horse_id"].transform("size").to_numpy()
    elif "n_horses" in df.columns:
        out[HRCols.N_HORSES] = pd.to_numeric(df["n_horses"], errors="coerce").to_numpy()
    # 通過順 → first_corner（脚質算出の入力）。add_pace_stats は first_corner/頭数 で _pace_num を出す。
    if HRCols.CORNER in df.columns:
        from src.preprocessing._horse_results_processor import parse_corner
        out["first_corner"] = df[HRCols.CORNER].map(lambda x: parse_corner(x, 1)).to_numpy()
    for src_col, dst in (("course_len", "course_len"), ("race_type", "race_type"),
                         (ResultsCols.KINRYO, HRCols.KINRYO), ("開催", HRCols.PLACE)):
        if src_col in df.columns:
            out[dst] = df[src_col].to_numpy()
    if "jockey_id" in df.columns:  # 乗替判定(jockey_change)の代理
        out[HRCols.JOCKEY] = df["jockey_id"].astype(str).to_numpy()

    # 実効馬場: 芝は ground_state1、ダートは ground_state2（add_type_ground_stats と同規約）
    gs1 = df["ground_state1"] if "ground_state1" in df.columns else None
    gs2 = df["ground_state2"] if "ground_state2" in df.columns else None
    ground = gs1 if gs1 is not None else gs2
    if gs1 is not None and gs2 is not None and "race_type" in df.columns:
        ground = gs1.where(df["race_type"] != Master.RACE_TYPE_DIRT, gs2)
    if ground is not None:
        out[HRCols.GROUND_STATE] = ground.to_numpy()

    out = out.dropna(subset=["horse_id", "date", HRCols.RANK])
    return out.set_index("horse_id")


def summarize(horse_results: pd.DataFrame, target_cols: list) -> pd.DataFrame:
    """§2i: horse_id ごとに target_cols を AGG_STATS で多統計量集計する。

    返り値の列名形式: {col}_{stat}（例: 着順_mean, 着順_std）
    呼び出し元で .add_suffix("_5R") 等を付与する。

    target_cols に string dtype の列（DB復元で混入しうる）が来ても落ちないよう、
    集計前に数値へ強制変換する（mean/std 等の reduction が str で失敗するのを防ぐ）。
    """
    num = horse_results[target_cols].apply(pd.to_numeric, errors="coerce")
    agg = num.groupby(level=0).agg(AGG_STATS)
    agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
    return agg


def summarize_with(
    horse_results: pd.DataFrame, target_cols: list, group_col: str
) -> pd.DataFrame:
    """(horse_id, group_col) ごとに target_cols を AGG_STATS で集計する。"""
    agg = horse_results.groupby(["horse_id", group_col])[target_cols].agg(AGG_STATS)
    agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
    return agg


def add_pace_stats(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """直近 N レースの脚質(走法)集計特徴量（pace_median / leg_type_binary / pace_at_distance）を追加。

    脚質は「ペース」列（レースのペース＝タイム文字列）ではなく通過順（第1コーナー位置）
    から導く。``_pace_num = first_corner / 頭数 ∈ [0,1]``（0=逃げ/前、1=追込/後）。
    旧実装は 'ペース'(6千種のタイム文字列)を脚質カテゴリ表(逃先差追)で map しており
    全 NaN だった（重要度0%の原因）。first_corner は HorseResultsProcessor が通過順から
    パース済み。
    """
    n_horses_col = HRCols.N_HORSES  # '頭数'
    if "first_corner" not in horse_results.columns or n_horses_col not in horse_results.columns:
        return results

    hr = horse_results.copy()
    fc = pd.to_numeric(hr["first_corner"], errors="coerce")
    nh = pd.to_numeric(hr[n_horses_col], errors="coerce")
    hr["_pace_num"] = (fc / nh).clip(lower=0.0, upper=1.0)

    # Overall pace median over last N races
    n_hr = filter_horse_results(hr, PACE_RECENT_N)
    pace_median = n_hr.groupby(level=0)["_pace_num"].median().rename("pace_median")

    # leg_type_binary: 前半(<0.5)=0(逃げ・先行), 後半(>=0.5)=1(差し・追込)
    def _to_binary(v: float) -> float:
        if pd.isna(v):
            return float("nan")
        return 0.0 if v < 0.5 else 1.0

    leg_binary = pace_median.map(_to_binary).rename("leg_type_binary")

    results = results.merge(pace_median, left_on="horse_id", right_index=True, how="left")
    results = results.merge(leg_binary, left_on="horse_id", right_index=True, how="left")

    # pace_at_distance: 同距離帯(±100m = ±1 in 100m units)での脚質中央値
    if "course_len" not in results.columns or "course_len" not in hr.columns:
        return results

    current_info = results[["horse_id", "course_len"]].drop_duplicates("horse_id")
    hr_reset = hr.reset_index()
    hr_with_cur = hr_reset.merge(
        current_info, on="horse_id", suffixes=("_past", "_cur")
    )
    at_dist = hr_with_cur[
        abs(hr_with_cur["course_len_past"] - hr_with_cur["course_len_cur"]) <= 1
    ]
    pace_at_dist = at_dist.groupby("horse_id")["_pace_num"].median().rename("pace_at_distance")
    results = results.merge(pace_at_dist, left_on="horse_id", right_index=True, how="left")

    return results


def add_growth_stats(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """成長/フォーム・トレンド特徴量を追加する（早熟/晩成の客観代理、リーク無し）。

    ``growth_trend = 直近3走の平均相対着順 − それ以前の平均相対着順``。相対着順は
    ``着順/頭数`` で 0=勝ち〜1=最下位。負＝直近の方が良い＝上昇基調（成長/復調）、
    正＝直近が悪い＝下降。年齢とともに良化する馬（晩成）を捉える。``n_starts``（出走数）
    も付与してキャリアの厚みを表す。horse_results は当該レース日より前のみ（リーク無し）。
    """
    rank_col = HRCols.RANK  # '着順'
    n_horses_col = HRCols.N_HORSES  # '頭数'
    if (
        horse_results.empty
        or rank_col not in horse_results.columns
        or n_horses_col not in horse_results.columns
    ):
        return results

    hr = horse_results.copy()
    rr = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(hr[n_horses_col], errors="coerce")
    hr["_rr"] = rr
    hr = hr.sort_values("date")
    # 馬ごとの新しい順インデックス（0=最新）。ベクトル化して per-group apply を避ける。
    hr["_ridx"] = hr.groupby(level=0).cumcount(ascending=False)
    recent = hr[hr["_ridx"] < 3].groupby(level=0)["_rr"].mean()
    older = hr[hr["_ridx"] >= 3].groupby(level=0)["_rr"].mean()
    growth = (recent - older).rename("growth_trend")
    n_starts = hr.groupby(level=0)["_rr"].count().rename("n_starts")

    results = results.merge(growth, left_on="horse_id", right_index=True, how="left")
    results = results.merge(n_starts, left_on="horse_id", right_index=True, how="left")
    return results


def add_prev_race_features(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """前走との比較特徴（距離延長/短縮・斤量増減・乗り替わり）を追加する。

    前走＝最も新しい過去走（horse_results は当該レース日より前のみなのでリーク無し）。
    - ``dist_change``       : 今回 course_len − 前走 course_len（正=延長・負=短縮）
    - ``dist_change_ratio`` : dist_change ÷ 前走距離（相対距離変化。基準距離依存の負荷を表現）
    - ``kinryo_delta``      : 今回 斤量 − 前走 斤量（ハンデ増減）
    - ``jockey_change``     : 騎手が前走から替わったか（1=乗り替わり、0=継続、初出走=NaN）
    """
    if horse_results.empty:
        return results

    hr = horse_results.sort_values("date")
    prev = hr.groupby(level=0).tail(1)  # 馬ごとの最新（=前走）1行
    rename: dict = {}
    if "course_len" in prev.columns:
        rename["course_len"] = "_prev_course_len"
    if HRCols.KINRYO in prev.columns:
        rename[HRCols.KINRYO] = "_prev_kinryo"
    if HRCols.JOCKEY in prev.columns:
        rename[HRCols.JOCKEY] = "_prev_jockey"
    if not rename:
        return results

    prev_sub = prev[list(rename)].rename(columns=rename)
    results = results.merge(prev_sub, left_on="horse_id", right_index=True, how="left")

    if "_prev_course_len" in results.columns and "course_len" in results.columns:
        prev_len = pd.to_numeric(results["_prev_course_len"], errors="coerce")
        results["dist_change"] = (
            pd.to_numeric(results["course_len"], errors="coerce") - prev_len
        )
        # 相対距離変化（前走比）。基準距離で割り、延長/短縮の体力負荷を非線形に表現。
        # 0除算は NaN（前走距離0は実データ上ありえないが安全側）。
        results["dist_change_ratio"] = results["dist_change"] / prev_len.where(prev_len != 0)
    if "_prev_kinryo" in results.columns and HRCols.KINRYO in results.columns:
        results["kinryo_delta"] = (
            pd.to_numeric(results[HRCols.KINRYO], errors="coerce")
            - pd.to_numeric(results["_prev_kinryo"], errors="coerce")
        )
    if "_prev_jockey" in results.columns and HRCols.JOCKEY in results.columns:
        cur_j = results[HRCols.JOCKEY].astype(str).str.strip()
        prev_j = results["_prev_jockey"].astype(str).str.strip()
        jc = (cur_j != prev_j).astype(float)
        jc[results["_prev_jockey"].isna()] = float("nan")  # 初出走は欠損
        results["jockey_change"] = jc

    results = results.drop(
        columns=[c for c in ("_prev_course_len", "_prev_kinryo", "_prev_jockey") if c in results.columns],
        errors="ignore",
    )
    return results


def add_aptitude_stats(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """馬場・競馬場の適性特徴を追加する（リーク無し）。

    - ``wet_win_rate`` / ``wet_rel_rank`` : 道悪（馬場∈稍重/重/不良）での勝率・相対着順。
      今回の馬場に依らず「この馬の道悪実績」を表す（モデルが当日馬場ダミーと併用）。
    - ``place_win_rate`` : 今回と同じ競馬場（開催）での過去勝率（東京専用機/中山の鬼を捕捉）。
    horse_results は当該レース日より前のみ（リーク無し）。
    """
    rank_col = HRCols.RANK  # '着順'
    n_horses_col = HRCols.N_HORSES  # '頭数'
    ground_col = HRCols.GROUND_STATE  # '馬場'
    place_col = HRCols.PLACE  # '開催'
    if horse_results.empty or rank_col not in horse_results.columns:
        return results

    hr = horse_results.copy()
    hr["_is_win"] = (pd.to_numeric(hr[rank_col], errors="coerce") == 1).astype(float)
    if n_horses_col in hr.columns:
        hr["_rel_rank"] = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(
            hr[n_horses_col], errors="coerce"
        )

    # 道悪（非・良）実績: 当日馬場に依らない馬固有の適性
    if ground_col in hr.columns:
        wet = hr[hr[ground_col].astype(str).isin(["稍重", "重", "不良"])]
        if not wet.empty:
            wet_win = wet.groupby(level=0)["_is_win"].mean().rename("wet_win_rate")
            results = results.merge(wet_win, left_on="horse_id", right_index=True, how="left")
            if "_rel_rank" in wet.columns:
                wet_rr = wet.groupby(level=0)["_rel_rank"].mean().rename("wet_rel_rank")
                results = results.merge(wet_rr, left_on="horse_id", right_index=True, how="left")

    # 競馬場別実績: 今回と同じ開催での過去勝率。開催コードは horse_results が
    # ゼロ詰め文字列("05")・race_info が整数(5) と表現が異なるため数値化して比較する。
    if place_col in hr.columns and place_col in results.columns:
        hr_reset = hr.reset_index()
        hr_reset["_place"] = pd.to_numeric(hr_reset[place_col], errors="coerce")
        cur = results[["horse_id", place_col]].drop_duplicates("horse_id")
        cur["_cur_place"] = pd.to_numeric(cur[place_col], errors="coerce")
        merged = hr_reset.merge(cur[["horse_id", "_cur_place"]], on="horse_id")
        same = merged[merged["_place"] == merged["_cur_place"]]
        if not same.empty:
            place_win = same.groupby("horse_id")["_is_win"].mean().rename("place_win_rate")
            results = results.merge(place_win, left_on="horse_id", right_index=True, how="left")

        # 回り(右/左)適性: 開催コード→回り方向へ写像し、今回と同じ回り方向での過去平均相対着順。
        # 低い=その回りが得意（東京巧者=左得意 等を競馬場個別でなく方向一般で捕捉）。
        # place_win_rate（同一場）より粗いが汎化する。horse_results は当該日より前のみ＝リーク無し。
        if "_rel_rank" in merged.columns:
            from src.constants._master import Master
            amap = Master.PLACE_AROUND
            a_past = merged["_place"].map(lambda c: amap.get(int(c)) if pd.notna(c) else None)
            a_cur = merged["_cur_place"].map(lambda c: amap.get(int(c)) if pd.notna(c) else None)
            same_dir = merged[(a_past == a_cur) & a_cur.notna()]
            if not same_dir.empty:
                around_rr = (same_dir.groupby("horse_id")["_rel_rank"].mean()
                             .rename("around_rel_rank"))
                results = results.merge(around_rr, left_on="horse_id", right_index=True, how="left")

    return results


def add_speed_figure_stats(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """スピード指数（タイム偏差）の集計を追加する（リーク無し）。

    speed_figure は HorseResultsProcessor が各過去走に付与済み（基準タイムから何σ速いか、
    faster=正）。ここでは馬ごとに:
    - ``speed_fig_best``  : 過去最高指数（ピーク能力＝この馬の地力上限）
    - ``speed_fig_mean5`` : 直近5走平均（現在の調子・近走の地力）
    を算出する。horse_results は当該レース日より前のみ（リーク無し）。
    """
    if horse_results.empty or "speed_figure" not in horse_results.columns:
        return results

    hr = horse_results
    best = hr.groupby(level=0)["speed_figure"].max().rename("speed_fig_best")
    recent5 = filter_horse_results(hr, 5)
    mean5 = recent5.groupby(level=0)["speed_figure"].mean().rename("speed_fig_mean5")

    results = results.merge(best, left_on="horse_id", right_index=True, how="left")
    results = results.merge(mean5, left_on="horse_id", right_index=True, how="left")
    return results


def add_career_stats(results: pd.DataFrame, horse_results: pd.DataFrame) -> pd.DataFrame:
    """as-of キャリア累計（出走数/勝利数/勝率/獲得賞金）を追加する（リーク無し）。

    horse_info ページの「現在の通算成績・獲得賞金」はスクレイプ時点＝当該レースより
    未来の走を含むためリーク（かつ train/serve skew）になる。代わりに horse_results を
    **当該レース日より前**（`_merge_horse_results` が date でカット済み）で積算し、
    各レース時点での過去キャリアを再現する。学習・推論で同一計算となり skew も消える。
    """
    import numpy as np

    if horse_results.empty or HRCols.RANK not in horse_results.columns:
        return results

    hr = horse_results
    starts = hr.groupby(level=0).size().rename("career_starts")
    is_win = (hr[HRCols.RANK] == 1).astype(float)
    wins = is_win.groupby(level=0).sum().rename("career_wins")
    results = results.merge(starts, left_on="horse_id", right_index=True, how="left")
    results = results.merge(wins, left_on="horse_id", right_index=True, how="left")
    # 勝率（過去走 0 の初出走馬は starts/wins とも欠損 → 勝率も NaN のまま＝未知）
    results["career_winrate"] = results["career_wins"] / results["career_starts"]

    # 獲得賞金（過去走の賞金合計）。桁が大きく裾が重いので log1p で圧縮した列も持つ
    if HRCols.PRIZE in hr.columns:
        earnings = hr.groupby(level=0)[HRCols.PRIZE].sum().rename("career_earnings")
        results = results.merge(earnings, left_on="horse_id", right_index=True, how="left")
        results["career_earnings_log"] = np.log1p(results["career_earnings"].fillna(0.0))
    return results


def add_recent_form_stats(
    results: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.DataFrame:
    """直近 N レースの成績「率」を追加する（リーク無し）。

    §2i の多窓集計（着順_mean_5R 等）が分布統計（mean/std/...）なのに対し、ここでは
    近走フォームの直感的指標を窓ごと（N_RACES_LIST=5/9/20）に算出する:
    - ``win_rate_NR``      : 直近 N 走の勝率（着順==1 の割合）
    - ``rentai_rate_NR``   : 直近 N 走の連対率（着順<=2 の割合）
    - ``place_rate_NR``    : 直近 N 走の複勝率（着順<=3 の割合。Place ヘッドの top3 と整合）
    - ``avg_rel_rank_NR``  : 直近 N 走の平均相対着順（着順/頭数。頭数差を補正）

    horse_results は当該レース日より前のみ（_merge_horse_results が date でカット済み）。
    過去走の無い馬・窓に満たない馬は該当走数だけで率を出す（履歴ゼロは NaN）。
    """
    rank_col = HRCols.RANK  # '着順'
    n_horses_col = HRCols.N_HORSES  # '頭数'
    if horse_results.empty or rank_col not in horse_results.columns:
        return results

    hr = horse_results.copy()
    rank = pd.to_numeric(hr[rank_col], errors="coerce")
    hr["_win"] = (rank == 1).astype(float)
    hr["_rentai"] = (rank <= 2).astype(float)
    hr["_place"] = (rank <= 3).astype(float)
    has_rel = n_horses_col in hr.columns
    if has_rel:
        hr["_rel"] = rank / pd.to_numeric(hr[n_horses_col], errors="coerce")

    for n in N_RACES_LIST:
        recent = filter_horse_results(hr, n)
        g = recent.groupby(level=0)
        agg_map = {
            f"win_rate_{n}R": ("_win", "mean"),
            f"rentai_rate_{n}R": ("_rentai", "mean"),
            f"place_rate_{n}R": ("_place", "mean"),
        }
        if has_rel:
            agg_map[f"avg_rel_rank_{n}R"] = ("_rel", "mean")
        summarized = g.agg(**agg_map)
        results = results.merge(
            summarized, left_on="horse_id", right_index=True, how="left"
        )
    return results


def add_opponent_strength_stats(
    results: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.DataFrame:
    """相手強度（軽量代理）: 過去に走ったレースの格を ordinal 化して集計する（リーク無し）。

    名寄せ不要。horse_results のレース名から grade を抽出し、各馬の過去走（当該レース日
    より前にカット済み）で集計する。
    - ``faced_grade_max``    : これまでに走った最高グレード（実力の天井の代理）
    - ``faced_grade_mean``   : 平均グレード（普段戦っている相手レベル）
    - ``faced_graded_count`` : 重賞(G3 以上)出走回数
    前走履歴なしの馬は全特徴 NaN（未知＝安全な欠損）。
    """
    from src.preprocessing._entity_resolver import extract_race_grade

    name_col = HRCols.RACE_NAME
    if horse_results.empty or name_col not in horse_results.columns:
        return results

    hr = horse_results
    names = hr[name_col].astype(str)
    # ユニークなレース名だけ grade 解決（重複名の正規表現コストを回避）
    grade_by_name = {
        n: GRADE_ORDINAL.get(extract_race_grade(n) or "", 0) for n in names.unique()
    }
    faced = names.map(grade_by_name)

    g = faced.groupby(level=0)
    results = results.merge(
        g.max().rename("faced_grade_max"), left_on="horse_id", right_index=True, how="left"
    )
    results = results.merge(
        g.mean().rename("faced_grade_mean"), left_on="horse_id", right_index=True, how="left"
    )
    graded = (faced >= 3).astype(float).groupby(level=0).sum().rename("faced_graded_count")
    results = results.merge(graded, left_on="horse_id", right_index=True, how="left")
    return results


def add_course_condition_stats(
    results: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.DataFrame:
    """コース長別・コース種別別の過去成績を追加する（リーク無し）。

    今回のコース条件に合わせた「この馬の適性」を表す:
    - ``win_rate_at_distance``    : 同距離帯(±100m)での勝率
    - ``avg_rank_at_distance``    : 同距離帯での平均相対着順（着順/頭数。小さいほど好成績）
    - ``n_runs_at_distance``      : 同距離帯での出走数（経験量。少数集計の信頼度の手掛り）
    - ``avg_rank_at_course_type`` : 同コース種別(芝/ダート)での平均相対着順
    - ``win_rate_at_course_type`` : 同コース種別での勝率
    前走履歴が条件に合致しない馬は該当特徴 NaN（未知＝安全な欠損）。
    """
    rank_col = HRCols.RANK  # '着順'
    n_horses_col = HRCols.N_HORSES  # '頭数'
    if horse_results.empty or rank_col not in horse_results.columns:
        return results
    if "course_len" not in results.columns:
        return results

    hr = horse_results.copy()
    hr["_is_win"] = (hr[rank_col] == 1).astype(float)
    if n_horses_col in hr.columns:
        hr["_rel_rank"] = hr[rank_col] / hr[n_horses_col]

    # Build per-horse current race info for distance/type filtering
    info_cols = ["horse_id", "course_len"]
    if "race_type" in results.columns:
        info_cols.append("race_type")
    current_info = results[info_cols].drop_duplicates("horse_id")

    hr_reset = hr.reset_index()
    hr_with_cur = hr_reset.merge(
        current_info, on="horse_id", suffixes=("_past", "_cur")
    )

    # win_rate_at_distance / avg_rank_at_distance / n_runs_at_distance: ±100m (±1 unit)
    at_dist = hr_with_cur[
        abs(hr_with_cur["course_len_past"] - hr_with_cur["course_len_cur"]) <= 1
    ]
    dist_grp = at_dist.groupby("horse_id")
    win_rate = dist_grp["_is_win"].mean().rename("win_rate_at_distance")
    results = results.merge(win_rate, left_on="horse_id", right_index=True, how="left")
    n_runs_dist = dist_grp.size().rename("n_runs_at_distance")
    results = results.merge(n_runs_dist, left_on="horse_id", right_index=True, how="left")
    if "_rel_rank" in at_dist.columns:
        avg_rank_dist = dist_grp["_rel_rank"].mean().rename("avg_rank_at_distance")
        results = results.merge(avg_rank_dist, left_on="horse_id", right_index=True, how="left")

    # avg_rank_at_course_type / win_rate_at_course_type: same race_type
    if (
        "race_type_past" in hr_with_cur.columns
        and "race_type_cur" in hr_with_cur.columns
    ):
        at_type = hr_with_cur[hr_with_cur["race_type_past"] == hr_with_cur["race_type_cur"]]
        type_grp = at_type.groupby("horse_id")
        win_type = type_grp["_is_win"].mean().rename("win_rate_at_course_type")
        results = results.merge(win_type, left_on="horse_id", right_index=True, how="left")
        if "_rel_rank" in at_type.columns:
            avg_rank = type_grp["_rel_rank"].mean().rename("avg_rank_at_course_type")
            results = results.merge(avg_rank, left_on="horse_id", right_index=True, how="left")

    return results


def add_type_ground_stats(
    results: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.DataFrame:
    """レース種別 × 馬場状態の組合せ別の過去成績を追加する（リーク無し）。

    今回のレース種別(芝/ダート)と馬場状態(良/稍重/重/不良)に合致する過去走だけで
    集計し、「この条件でのこの馬の適性」を表す:
    - ``win_rate_type_ground`` : 同種別×同馬場での勝率
    - ``avg_rank_type_ground`` : 同種別×同馬場での平均相対着順
    - ``n_runs_type_ground``   : 同種別×同馬場での出走数

    今回の馬場は種別で使い分ける（芝→ground_state1 / ダート→ground_state2、
    無ければ ground_state1）。過去走の馬場は horse_results の '馬場'。
    前走履歴が条件に合致しない馬は NaN（未知＝安全な欠損）。
    """
    from src.constants._master import Master as _M

    rank_col = HRCols.RANK
    n_horses_col = HRCols.N_HORSES
    ground_col = HRCols.GROUND_STATE  # '馬場'
    if horse_results.empty or rank_col not in horse_results.columns:
        return results
    if "race_type" not in results.columns or ground_col not in horse_results.columns:
        return results

    # 今回の実効馬場: 芝は ground_state1、ダートは ground_state2（無ければ gs1）
    gs1 = results["ground_state1"] if "ground_state1" in results.columns else None
    gs2 = results["ground_state2"] if "ground_state2" in results.columns else None
    cur_ground = gs1 if gs1 is not None else gs2
    if cur_ground is None:
        return results
    if gs1 is not None and gs2 is not None:
        cur_ground = gs1.where(results["race_type"] != _M.RACE_TYPE_DIRT, gs2)

    cur = results[["horse_id", "race_type"]].copy()
    cur["_cur_ground"] = cur_ground.astype(str)
    cur = cur.drop_duplicates("horse_id")

    hr = horse_results.copy()
    hr["_is_win"] = (pd.to_numeric(hr[rank_col], errors="coerce") == 1).astype(float)
    if n_horses_col in hr.columns:
        hr["_rel_rank"] = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(
            hr[n_horses_col], errors="coerce"
        )
    hr_reset = hr.reset_index()
    if "race_type" not in hr_reset.columns:
        return results
    merged = hr_reset.merge(cur, on="horse_id", suffixes=("_past", "_cur"))
    match = merged[
        (merged["race_type_past"] == merged["race_type_cur"])
        & (merged[ground_col].astype(str) == merged["_cur_ground"])
    ]
    if match.empty:
        return results
    grp = match.groupby("horse_id")
    results = results.merge(
        grp["_is_win"].mean().rename("win_rate_type_ground"),
        left_on="horse_id", right_index=True, how="left",
    )
    results = results.merge(
        grp.size().rename("n_runs_type_ground"),
        left_on="horse_id", right_index=True, how="left",
    )
    if "_rel_rank" in match.columns:
        results = results.merge(
            grp["_rel_rank"].mean().rename("avg_rank_type_ground"),
            left_on="horse_id", right_index=True, how="left",
        )
    return results


def add_race_class_stats(
    results: pd.DataFrame, horse_results: pd.DataFrame
) -> pd.DataFrame:
    """レースクラス（格）別の過去成績を追加する（リーク無し）。

    過去走のレース名から格を ``classify_race_class`` で判定し順序値化
    （新馬/未勝利=1 … G1=9）、今回のクラス（race_class 列）と突き合わせる:
    - ``win_rate_same_class``  : 今回と同格での勝率
    - ``avg_rank_same_class``  : 今回と同格での平均相対着順
    - ``n_runs_same_class``    : 今回と同格での出走数
    - ``win_rate_higher_class``: 今回以上の格での勝率（格上で勝てる＝真に強い馬の代理）
    - ``best_class_won``       : これまでに勝利した最高クラスの順序値（実績の天井）

    race_class 列（race_info 由来）が無いレースは NaN（未知＝安全な欠損）。
    """
    from src.constants._master import classify_race_class, race_class_level

    rank_col = HRCols.RANK
    n_horses_col = HRCols.N_HORSES
    name_col = HRCols.RACE_NAME  # 'レース名'
    if horse_results.empty or rank_col not in horse_results.columns:
        return results
    if "race_class" not in results.columns or name_col not in horse_results.columns:
        return results

    # 今回クラスの順序値（馬ごと）。race_class_level は不明クラスに None を返すため
    # numeric 強制（None→NaN）で列を float64 に保つ。object 混在のままだと best_class_won
    # 等が object dtype になり LightGBM が "pandas dtypes must be int/float/bool" で落ちる。
    cur = results[["horse_id", "race_class"]].drop_duplicates("horse_id").copy()
    cur["_cur_level"] = pd.to_numeric(cur["race_class"].map(race_class_level), errors="coerce")

    hr = horse_results.copy()
    hr["_is_win"] = (pd.to_numeric(hr[rank_col], errors="coerce") == 1).astype(float)
    if n_horses_col in hr.columns:
        hr["_rel_rank"] = pd.to_numeric(hr[rank_col], errors="coerce") / pd.to_numeric(
            hr[n_horses_col], errors="coerce"
        )
    # ユニークなレース名だけ格判定（正規表現コストの重複回避）。旧年代の分類不能な
    # レース名は None を返すため numeric 強制（None→NaN）で _past_level を float64 に保つ。
    # object のままだと best_class_won = groupby.max() が object になり学習で落ちる。
    names = hr[name_col].astype(str)
    level_by_name = {n: race_class_level(classify_race_class(n)) for n in names.unique()}
    hr["_past_level"] = pd.to_numeric(names.map(level_by_name), errors="coerce")

    hr_reset = hr.reset_index()
    merged = hr_reset.merge(cur[["horse_id", "_cur_level"]], on="horse_id")

    # best_class_won: 勝利した過去走の最高クラス（今回クラスに依存しない）
    won = merged[merged["_is_win"] == 1.0]
    if not won.empty:
        best_won = won.groupby("horse_id")["_past_level"].max().rename("best_class_won")
        results = results.merge(best_won, left_on="horse_id", right_index=True, how="left")

    # 同格・格上は今回クラスが判明している馬のみ対象
    known = merged[merged["_cur_level"].notna() & merged["_past_level"].notna()]
    if not known.empty:
        same = known[known["_past_level"] == known["_cur_level"]]
        if not same.empty:
            sg = same.groupby("horse_id")
            results = results.merge(
                sg["_is_win"].mean().rename("win_rate_same_class"),
                left_on="horse_id", right_index=True, how="left",
            )
            results = results.merge(
                sg.size().rename("n_runs_same_class"),
                left_on="horse_id", right_index=True, how="left",
            )
            if "_rel_rank" in same.columns:
                results = results.merge(
                    sg["_rel_rank"].mean().rename("avg_rank_same_class"),
                    left_on="horse_id", right_index=True, how="left",
                )
        higher = known[known["_past_level"] >= known["_cur_level"]]
        if not higher.empty:
            results = results.merge(
                higher.groupby("horse_id")["_is_win"].mean().rename("win_rate_higher_class"),
                left_on="horse_id", right_index=True, how="left",
            )
    return results
