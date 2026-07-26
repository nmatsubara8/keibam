"""JRA-VAN 系レース結果 CSV（1986-2021, 163万行）を keibam の raw スキーマへ変換する seed 変換器。

audit_seed_ids.py で検証済みの前提に基づく:
  - レースID(12桁=年|場|回|日|R) は自己整合 → そのまま race_id に使う（netkeiba 一致は仮定しない自己完結コーパス）。
  - horse_id/jockey_id/... は CSV に無い → **名前から合成 ID を作る**（パイプライン改変不要）。
  - horse_id は「馬名だけ」でなく **キャリア連続性でセグメント化**して同名別馬を分ける
    （馬齢が減る/不可能な性別遷移で区切る。牡→セ=去勢は同一馬として継続）。

出力（既存スクレイプ済みコーパスを壊さないよう別ファイル）:
    data/raw/seed_results.pkl     … ResultsProcessor が読める raw_results 形式
    data/raw/seed_race_info.pkl   … RaceInfoProcessor が読める raw_race_info 形式

血統(peds)・上がり/コーナー等のページ専用特徴は本 MVP では変換しない（results/race_info のみ）。

使い方:
    python seed_from_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv"
    python seed_from_csv.py <csv> --limit 50000 --dry-run   # 小サンプルで検証
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

# --- CSV 実列名（analyze_seed_csv / audit_seed_ids で確認済み） ---
C_RACE_ID = "レースID"
C_DATE = "レース日付"
C_KAI = "開催回数"
C_TRACK_CODE = "競馬場コード"
C_TRACK_NAME = "競馬場名"
C_DAY = "開催日数"
C_RNO = "レース番号"
C_COND = "競争条件"
C_RACE_NAME = "レース名"
C_HURDLE = "障害区分"
C_SURFACE = "芝・ダート区分"
C_AROUND = "右左回り・直線区分"
C_DIST = "距離(m)"
C_WEATHER = "天候"
C_GS1 = "馬場状態1"
C_GS2 = "馬場状態2"
C_POST = "発走時刻"
C_RANK = "着順"
C_WAKU = "枠番"
C_UMABAN = "馬番"
C_NAME = "馬名"
C_SEX = "性別"
C_AGE = "馬齢"
C_KINRYO = "斤量"
C_JOCKEY = "騎手"
C_TIME = "タイム"
C_MARGIN = "着差"
C_AGARI = "上り"
C_C1 = "1コーナー"
C_C2 = "2コーナー"
C_C3 = "3コーナー"
C_C4 = "4コーナー"
C_PRIZE = "賞金(万円)"
C_ODDS = "単勝"
C_POP = "人気"
C_WEIGHT = "馬体重"
C_WDIFF = "場体重増減"
C_TRAINER = "調教師"
C_OWNER = "馬主"


def _read_csv(path: str, limit: int | None) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, nrows=limit, encoding=enc, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("CSV を読めませんでした（エンコーディング）。")


def _norm_sex(v) -> str:
    """性別を単一文字（牡/牝/セ）に正規化。'セン'/'騙' → 'セ'。"""
    s = str(v).strip()
    if not s or s == "nan":
        return ""
    c = s[0]
    return "セ" if c in ("セ", "騙", "せ") else c


def build_synthetic_horse_id(df: pd.DataFrame) -> pd.Series:
    """馬名をキャリア連続性でセグメント化し、同名別馬を分けた合成 horse_id（int）を返す。

    馬名ごとに日付順で走査し、(a) 馬齢が前行より減少、(b) 不可能な性別遷移（牡→セ 以外の変化）
    のいずれかで新セグメント＝別馬とみなす。名前×セグメントを factorize して整数 ID にする。
    ID は 1 始まりの小さい整数で、netkeiba の 10桁 horse_id とは桁数が違うため衝突しない。
    """
    d = df[[C_NAME, C_DATE, C_AGE, C_SEX]].copy()
    d["_date"] = pd.to_datetime(d[C_DATE], errors="coerce")
    d["_age"] = pd.to_numeric(d[C_AGE], errors="coerce")
    d["_sex"] = d[C_SEX].map(_norm_sex)
    # 安定ソート（同名内で日付順、元の行順を保持）
    order = d.sort_values([C_NAME, "_date"], kind="stable").index
    d = d.loc[order]

    g = d.groupby(C_NAME, sort=False)
    prev_age = g["_age"].shift()
    prev_sex = g["_sex"].shift()
    age_drop = d["_age"] < prev_age
    # 性別遷移: 前と異なり、かつ「牡→セ（去勢）」ではない → 別馬
    bad_sex = (d["_sex"] != prev_sex) & prev_sex.notna() & ~(
        (prev_sex == "牡") & (d["_sex"] == "セ")
    )
    new_seg = (age_drop | bad_sex).fillna(False)
    seg = new_seg.groupby(d[C_NAME], sort=False).cumsum().astype(int)
    key = d[C_NAME].astype(str) + "#" + seg.astype(str)
    codes = pd.factorize(key)[0] + 1  # 1 始まり
    return pd.Series(codes, index=d.index).reindex(df.index)


def _synth_id(series: pd.Series) -> pd.Series:
    """名前 → 1 始まりの合成整数 ID（欠損は 0）。"""
    codes, _ = pd.factorize(series.astype(str).where(series.notna(), None))
    return pd.Series(np.where(series.notna(), codes + 1, 0), index=series.index)


def _weight_str(df: pd.DataFrame) -> pd.Series:
    """馬体重(数値)+場体重増減(数値) → "468(+0)" 形式（体重欠損は None）。"""
    w = pd.to_numeric(df[C_WEIGHT], errors="coerce")
    wd = pd.to_numeric(df[C_WDIFF], errors="coerce").fillna(0)
    body = w.round().astype("Int64").astype(str)
    diff = wd.round().astype(int).map(lambda x: f"({x:+d})")
    return (body + diff).where(w.notna(), None)


def _corner_str(df: pd.DataFrame) -> list:
    """1〜4コーナーの自馬通過位置 → "3-3-2-1" 形式（欠損コーナーは省く）。"""
    arrs = [pd.to_numeric(df[c], errors="coerce").to_numpy() if c in df.columns else None
            for c in (C_C1, C_C2, C_C3, C_C4)]
    arrs = [a for a in arrs if a is not None]
    n = len(df)
    out = []
    for i in range(n):
        parts = [str(int(a[i])) for a in arrs if a[i] == a[i]]  # a[i]==a[i]: not NaN
        out.append("-".join(parts) if parts else None)
    return out


def build_results(df: pd.DataFrame, hid: pd.Series) -> pd.DataFrame:
    """CSV → raw_results（ResultsProcessor が読める形式）。hid=合成 horse_id（共有）。"""
    out = pd.DataFrame(index=df.index)
    out["race_id"] = df[C_RACE_ID].astype("Int64").astype(str)
    out["着順"] = pd.to_numeric(df[C_RANK], errors="coerce")  # NaN(中止/除外)は後段で drop
    out["枠番"] = pd.to_numeric(df[C_WAKU], errors="coerce")
    out["馬番"] = pd.to_numeric(df[C_UMABAN], errors="coerce")
    out["斤量"] = pd.to_numeric(df[C_KINRYO], errors="coerce")
    out["単勝"] = pd.to_numeric(df[C_ODDS], errors="coerce")
    out["人気"] = pd.to_numeric(df[C_POP], errors="coerce")
    # 性齢 = 性別(単一文字) + 馬齢 → "牡4"（ResultsProcessor が str[0]/str[1:] で分解）
    sex = df[C_SEX].map(_norm_sex)
    age = pd.to_numeric(df[C_AGE], errors="coerce").astype("Int64")
    out["性齢"] = sex.str.cat(age.astype(str).where(age.notna(), ""), na_rep="")
    # 馬体重 = "468(+0)"（ResultsProcessor が "(" で分解）。体重欠損は None。
    out["馬体重"] = _weight_str(df)
    # 名前系（デバッグ・将来用。ResultsProcessor は最終的に落とす）
    out["馬名"] = df[C_NAME]
    out["騎手"] = df[C_JOCKEY]
    out["タイム"] = df[C_TIME]
    out["着差"] = df[C_MARGIN]
    out["調教師"] = df[C_TRAINER]
    # 合成 ID（horse_id は共有、person は名前→整数）
    out["horse_id"] = hid.astype("Int64").astype(str)
    out["jockey_id"] = _synth_id(df[C_JOCKEY]).astype(str)
    out["trainer_id"] = _synth_id(df[C_TRAINER]).astype(str)
    out["owner_id"] = _synth_id(df[C_OWNER]).astype(str)
    out = out.set_index("race_id")
    return out


def _get(df: pd.DataFrame, col: str, numeric: bool = False) -> pd.Series:
    """列が無ければ NaN 列を返す安全 getter（任意列の欠損でクラッシュさせない）。"""
    if col not in df.columns:
        return pd.Series(pd.NA, index=df.index)
    return pd.to_numeric(df[col], errors="coerce") if numeric else df[col]


def build_horse_results(df: pd.DataFrame, hid: pd.Series) -> pd.DataFrame:
    """CSV → raw_horse_results（HorseResultsProcessor が読める形式）。

    各結果行＝その馬の 1 過去走レコード。form-from-results の劣化再構成と違い、CSV が持つ
    上り(NOBORI)/通過(CORNER)/賞金(PRIZE)/タイム(TIME) をそのまま載せるため、脚質・pace・
    speed_figure・賞金/class 系の form 特徴が全て有効化する。リーク回避は merger の date
    スライス（date<当該レース）に委ねる（過去走の事実のみ集計）。
    """
    from src.constants._horse_results_cols import HorseResultsCols as HR
    from src.constants._master import Master

    surface = df.apply(_race_type, axis=1)  # 芝/ダート/障害
    rt_char = surface.map({Master.RACE_TYPE_TURF: "芝", Master.RACE_TYPE_DIRT: "ダ",
                           Master.RACE_TYPE_HURDLE: "障"}).fillna("")
    dist = pd.to_numeric(df[C_DIST], errors="coerce").astype("Int64").astype(str)

    n_horses = df.groupby(C_RACE_ID)[C_UMABAN].transform("count")

    out = pd.DataFrame(index=df.index)
    out["horse_id"] = hid.astype("Int64").astype(str)
    out[HR.DATE] = pd.to_datetime(df[C_DATE], errors="coerce")
    out[HR.PLACE] = _get(df, C_TRACK_NAME)                 # 開催（"札幌"等→PLACE_DICT）
    out[HR.WEATHER] = _get(df, C_WEATHER)
    out[HR.R] = _get(df, C_RNO, numeric=True)
    out[HR.RACE_NAME] = _get(df, C_RACE_NAME)
    out[HR.N_HORSES] = n_horses.to_numpy()
    out[HR.WAKUBAN] = _get(df, C_WAKU, numeric=True)
    out[HR.UMABAN] = _get(df, C_UMABAN, numeric=True)
    out[HR.TANSHO_ODDS] = _get(df, C_ODDS, numeric=True)
    out[HR.POPULARITY] = _get(df, C_POP, numeric=True)
    out[HR.RANK] = _get(df, C_RANK, numeric=True)
    out[HR.JOCKEY] = _get(df, C_JOCKEY)                    # 乗替判定の代理（名前でよい）
    out[HR.KINRYO] = _get(df, C_KINRYO, numeric=True)
    out[HR.RACE_TYPE_COURSE_LEN] = rt_char.str.cat(dist.where(dist != "<NA>", ""))  # "ダ1500"
    out[HR.GROUND_STATE] = _get(df, C_GS1)
    out[HR.TIME] = _get(df, C_TIME)                        # "1:34.3"（processor が秒へ）
    out[HR.RANK_DIFF] = _get(df, C_MARGIN)
    out[HR.CORNER] = _corner_str(df)                       # "3-3-2-1"
    out[HR.PACE] = pd.NA                                   # レース pace は laptime(③)で後付け
    out[HR.NOBORI] = _get(df, C_AGARI, numeric=True)
    out[HR.WEIGHT_AND_DIFF] = _weight_str(df)
    out[HR.PRIZE] = _get(df, C_PRIZE, numeric=True)
    return out


def _race_type(row) -> str:
    """芝・ダート区分＋障害区分 → 芝/ダート/障害。"""
    from src.constants._master import Master

    if pd.notna(row.get(C_HURDLE)) and str(row.get(C_HURDLE)).strip():
        return Master.RACE_TYPE_HURDLE
    s = str(row.get(C_SURFACE, "")).strip()
    if s.startswith("芝"):
        return Master.RACE_TYPE_TURF
    if s.startswith("ダ"):
        return Master.RACE_TYPE_DIRT
    return s or None


def build_race_info(df: pd.DataFrame) -> pd.DataFrame:
    """CSV → raw_race_info（RaceInfoProcessor が読める形式）。レースごとに 1 行へ集約。"""
    from src.constants._master import classify_race_class

    # レース単位に集約（結果行の先頭を代表に）
    g = df.sort_values([C_RACE_ID]).groupby(C_RACE_ID, sort=False).first().reset_index()

    out = pd.DataFrame()
    out["race_id"] = g[C_RACE_ID].astype("Int64").astype(str)
    out["place_id"] = pd.to_numeric(g[C_TRACK_CODE], errors="coerce").astype("Int64")
    out["place"] = g[C_TRACK_NAME]
    out["days"] = pd.to_numeric(g[C_DAY], errors="coerce")
    out["times"] = pd.to_numeric(g[C_KAI], errors="coerce")
    # date は RaceInfoProcessor が "%Y年%m月%d日" を要求 → 変換
    d = pd.to_datetime(g[C_DATE], errors="coerce")
    out["date"] = d.dt.strftime("%Y年%m月%d日")
    out["time"] = g[C_POST]
    out["race_type"] = g.apply(_race_type, axis=1)

    def _around(x):
        # 欠損（pyarrow バックエンドでは float NaN で渡る）を安全に処理。
        if pd.isna(x):
            return None
        s = str(x).strip()
        return "直線" if s.startswith("直") else s

    out["around"] = g[C_AROUND].map(_around)
    out["course_len"] = pd.to_numeric(g[C_DIST], errors="coerce")
    out["weather"] = g[C_WEATHER]
    gs1 = g[C_GS1]
    out["ground_state1"] = gs1
    # gs2 は CSV では 100%欠損 → gs1 で埋める（ResultsProcessor 系の芝/ダート二値と整合）
    out["ground_state2"] = g[C_GS2].where(g[C_GS2].notna(), gs1)
    # race_class はレース名→競争条件の順で分類（scrape 版と同じ classify_race_class）
    rc = g[C_RACE_NAME].map(lambda x: classify_race_class(x) if pd.notna(x) else None)
    rc = rc.where(rc.notna(), g[C_COND].map(lambda x: classify_race_class(x) if pd.notna(x) else None))
    out["race_class"] = rc
    # age / sex 制限（競争条件から素朴に抽出。無ければ None）
    cond = g[C_COND].astype(str)
    out["age"] = cond.str.extract(r"(\d+)歳")[0].where(cond.str.contains("歳", na=False))
    out["age"] = np.where(cond.str.contains("以上", na=False) & out["age"].notna(),
                          out["age"].astype(str) + "+", out["age"])
    out["sex"] = np.where(cond.str.contains("牝", na=False), "牝", None)
    out["race_condition"] = g[C_COND]
    # race_id は **列のまま** 返す（RaceInfoProcessor が _preprocess 末尾で set_index("race_id")
    # するため。results は逆に race_id を index にする＝n_horses が index.value_counts に依存）。
    return out


def _validate(results_path: str, race_info_path: str, horse_results_path: str) -> None:
    """出力を実 Processor に通し、前処理が落ちないことを確認する。"""
    from src.preprocessing._horse_results_processor import HorseResultsProcessor
    from src.preprocessing._race_info_processor import RaceInfoProcessor
    from src.preprocessing._results_processor import ResultsProcessor

    r = ResultsProcessor(results_path).preprocessed_data
    ri = RaceInfoProcessor(race_info_path).preprocessed_data
    hr = HorseResultsProcessor(horse_results_path).preprocessed_data
    print(f"    [validate] ResultsProcessor OK: {len(r):,} 行")
    print(f"    [validate] RaceInfoProcessor OK: {len(ri):,} レース")
    print(f"    [validate] HorseResultsProcessor OK: {len(hr):,} 行 / "
          f"speed_figure 非null {hr['speed_figure'].notna().mean()*100:.0f}% / "
          f"賞金 非null {hr['賞金'].notna().mean()*100:.0f}%")


def run(args) -> int:
    if not os.path.isfile(args.path):
        print(f"[NG] ファイルが見つかりません: {args.path}（WSL は /mnt/c/... 形式）")
        return 2
    print("=" * 78)
    print(f"seed 変換: {os.path.basename(args.path)}  limit={args.limit or '全件'}")
    print("=" * 78)
    df = _read_csv(args.path, args.limit)
    print(f"読込: {len(df):,} 行 / {df.shape[1]} 列")

    hid = build_synthetic_horse_id(df)  # 合成 horse_id（results/horse_results で共有）
    results = build_results(df, hid)
    race_info = build_race_info(df)
    horse_results = build_horse_results(df, hid)

    n_horse = results["horse_id"].nunique()
    n_jockey = (results["jockey_id"] != "0").sum() and results["jockey_id"].nunique()
    dmin = pd.to_datetime(df[C_DATE], errors="coerce").min()
    dmax = pd.to_datetime(df[C_DATE], errors="coerce").max()
    print(f"  raw_results   : {len(results):,} 行 / 合成 horse_id {n_horse:,} 頭")
    print(f"  raw_race_info : {len(race_info):,} レース / 期間 {dmin.date()}〜{dmax.date()}")
    print(f"  合成 jockey_id: {results['jockey_id'].nunique():,} / trainer_id: "
          f"{results['trainer_id'].nunique():,} / owner_id: {results['owner_id'].nunique():,}")

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    results_path = os.path.join(out_dir, "seed_results.pkl")
    race_info_path = os.path.join(out_dir, "seed_race_info.pkl")
    horse_results_path = os.path.join(out_dir, "seed_horse_results.pkl")

    if args.dry_run:
        print("\n[dry-run] 書き込みは行いません。サンプル（raw_results 先頭3行）:")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(results.head(3).to_string())
        # dry-run でも一時保存して Processor 検証だけ行う
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            rp = os.path.join(td, "r.pkl"); rip = os.path.join(td, "ri.pkl"); hp = os.path.join(td, "hr.pkl")
            results.to_pickle(rp); race_info.to_pickle(rip); horse_results.to_pickle(hp)
            _validate(rp, rip, hp)
        return 0

    results.to_pickle(results_path)
    race_info.to_pickle(race_info_path)
    horse_results.to_pickle(horse_results_path)
    print(f"\n書き込み: {results_path} / {race_info_path} / {horse_results_path}")
    _validate(results_path, race_info_path, horse_results_path)
    print("=" * 78)
    print("注意: これは *別ファイル*（seed_*.pkl）です。既存 raw_results.pkl は無改変。")
    print("本コーパスへの統合（合成IDと netkeiba IDの整合・期間の切り分け）は次段で設計します。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="JRA-VAN 系 CSV → keibam raw スキーマ変換")
    ap.add_argument("path", help="CSV パス（WSL からは /mnt/c/... 形式）")
    ap.add_argument("--limit", type=int, default=None, help="先頭 N 行だけ変換（試行用）")
    ap.add_argument("--out-dir", default="data/raw", help="出力先ディレクトリ（既定 data/raw）")
    ap.add_argument("--dry-run", action="store_true", help="書き込まず検証だけ行う")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
