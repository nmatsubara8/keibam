"""H3 ソース契約 + coverage 監査 + リーク防止の機械チェック（性能評価より前の必須工程）。

H3a 馬別ペース適性 / H3b 馬別ラップ適性 を作る**前**に、各 raw 列の契約を固定し、strictly-prior
生成のリーク安全を機械検証する。予測性能は一切見ない（見る前に契約を確定するのが目的）。

出力:
  (1) ソース契約表: source table/column・発走前取得可否・coverage開始年・結合キー・
      strictly-prior条件・欠測処理。
  (2) coverage 監査: 各 raw 列の年別 非欠測率/unique（実データ由来）。
  (3) リーク防止チェック: strictly_prior_runs が未来/同レースを除外するか・horse_results の日付
      invariant・KYI pace_yosou が race 単位（発走前予想）か。

ローカル実行（raw は gitignore 成果物）。純部は tests/features/test_strictly_prior.py で検証済。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# 事前に文書化する契約（実データで検証・確定する。performance は見ない）
# 監査で判明: horse_results は race_id 列を持たず `ペース` 文字列も疎（0-74%）＝過去走のz結合に不適。
# JRDB KYI+SED は ketto(血統登録番号=安定 horse id)+race_id+ymd(日付) を持ち、pace_yosou(前日=発走前)/
# race_pace・uma_pace(実測z)/chakujun・kakutei_ninki(市場超過)/mae3f・ato3f(前後3F) が揃う。
# ⇒ H3 は **KYI+SED を ketto+race_id で結合**して作る（horse_results は使わない）。
# 「発走前取得可否」は**二時点**で分離する（ユーザ査読）: source race 発走時に既知か / target race
# 発走時に既知か。H3 で重要なのは右端＝target 発走時。ymd<target_ymd の過去走結果は target 予測時に
# は既に確定＝使用可（source 発走時には未知でも問題ない）。
CONTRACT = [
    # (hypothesis, table, column, known@source, known@target, join_keys, strictly_prior, missing)
    ("H3a", "KYI(前日)", "pace_yosou(H/M/S)", "YES", "YES(現レースの予想)",
     "(race_id,ketto)・race単位一定", "現レースの予想のみ", "空→P_r(z)欠測→適性NaN(安全)"),
    ("H3a", "SED", "race_pace/uma_pace(実測z)", "NO", "YES(過去走のみ)",
     "ketto+race_id・ymd順序", "ymd<target & race_id≠target", "無→その過去走を除外"),
    ("H3a", "SED", "chakujun/kakutei_ninki", "NO", "YES(過去走のみ)",
     "ketto+race_id", "ymd<target & race_id≠target", "欠損走は残差から除外"),
    ("H3b", "SED", "mae3f_time/ato3f_time", "NO", "YES(過去走のみ)",
     "ketto+race_id", "ymd<target & race_id≠target", "欠損走は集約から除外"),
    ("H3b", "SED", "kyori/shiba_dirt/baba_state", "NO", "YES(過去走のみ)",
     "ketto+race_id", "ymd<target & race_id≠target", "層不明→全体基準へ縮約"),
    ("H3b", "SED(race相対)", "上り race内残差/percentile", "NO", "YES(過去走のみ・race_idで同走比較)",
     "ketto+race_id(+race集計)", "ymd<target & race_id≠target", "基準は必ず過去走のみで構築"),
    ("不使用", "horse_results", "ペース/上り(race_id無)", "—", "—",
     "horse_id のみ・race_id無", "—", "H3では使わない(KYI+SEDで代替)"),
]

# H3 が実際に使う SED 列（年別 coverage を出す）
SED_H3_COLS = ["ketto", "chakujun", "kakutei_ninki", "kyori", "shiba_dirt", "baba_state",
               "race_pace", "uma_pace", "mae3f_time", "ato3f_time", "ten_idx", "agari_idx"]


def _load_horse_results():
    import pandas as pd
    from src.constants._local_paths import LocalPaths
    for attr in ("RAW_HORSE_RESULTS_PATH", "HTML_HORSE_RESULTS_PATH"):
        p = getattr(LocalPaths, attr, "")
        if p and Path(p).exists():
            try:
                return pd.read_pickle(p), p
            except Exception as e:  # noqa: BLE001
                print(f"  [警告] {attr} 読込失敗: {e}", file=sys.stderr)
    return None, None


def _load_jrdb(rt):
    try:
        from src.jrdb._store import JrdbStore
        df = JrdbStore().read(rt)
        return df if df is not None and len(df) else None
    except Exception as e:  # noqa: BLE001
        print(f"  [情報] JRDB {rt} 読込不可（未取込かも）: {e}", file=sys.stderr)
        return None


def _date_col(df):
    for c in ("日付", "date", "DATE"):
        if c in df.columns:
            return c
    return None


def _year_series(df, date_col):
    import pandas as pd
    d = pd.to_datetime(df[date_col], errors="coerce")
    return d.dt.year


def _coverage_table(df, cols, year):
    """列×年の 非欠測率/unique を出す（raw 由来・結果不変の coverage 監査）。分母(行数)も併記。"""
    import pandas as pd
    years = sorted({int(y) for y in year.dropna().unique()})
    rowcnt = {int(y): int((year == y).sum()) for y in years}
    print(f"  {'(行数/年)':<20}" + "".join(f"{rowcnt[y]:>10,}" for y in years))
    print(f"  {'列':<20}" + "".join(f"{y:>10}" for y in years))
    for c in cols:
        if c not in df.columns:
            print(f"  {c:<20}" + "  (列なし)")
            continue
        s = pd.to_numeric(df[c], errors="coerce") if df[c].dtype.kind in "biufc" else df[c]
        cells = []
        for y in years:
            sub = df.loc[(year == y).to_numpy(), c]
            nm = float(sub.notna().mean()) if len(sub) else 0.0
            cells.append(f"{nm:.2f}")
        print(f"  {c:<20}" + "".join(f"{v:>10}" for v in cells))


def _identity_stats(df, id_col, date_col):
    """馬ID(ketto)の同定統計: 非欠測/unique/走数中央値・最大/2日付以上の馬数/同一馬×同一日重複。"""
    import pandas as pd
    if id_col not in df.columns:
        print(f"  [NG] ID列 '{id_col}' が無い。")
        return
    ids = df[id_col].astype(str)
    nonblank = ids.str.strip().replace("", pd.NA)
    d = pd.to_datetime(df[date_col], errors="coerce",
                       format="%Y%m%d" if df[date_col].astype(str).str.len().median() == 8 else None)
    runs = ids.groupby(ids).size()
    n_dates = d.groupby(ids).nunique()
    multi = int((n_dates >= 2).sum())
    dup = int(df.groupby([id_col, d]).size().gt(1).sum())
    print(f"  ID='{id_col}': 非欠測率={float(nonblank.notna().mean()):.4f}  unique={ids.nunique():,}  "
          f"走数 中央値={int(runs.median())}/最大={int(runs.max())}")
    print(f"  2日付以上の馬={multi:,}（strictly-prior 検証対象）  同一馬×同一日 重複={dup:,}"
          f"（0 なら date<target で安全）")


def _pace_yosou_race_coverage(kyi):
    """pace_yosou の**空文字正規化後** race単位 coverage と真の競合を出す（見かけの不一致を排除）。"""
    import pandas as pd
    if "pace_yosou" not in kyi.columns or "race_id" not in kyi.columns:
        print("  pace_yosou/race_id 列が無い。")
        return
    v = kyi["pace_yosou"].astype(str).str.strip()
    nonblank = v.replace("", pd.NA)
    k = kyi[["race_id"]].copy()
    k["v"] = nonblank
    g = k.groupby("race_id")["v"]
    has_nonblank = g.apply(lambda s: s.notna().any())
    all_blank = float((~has_nonblank).mean())
    # 非空値だけで競合するか（真の不一致）
    nuniq_nonblank = g.apply(lambda s: s.dropna().nunique())
    true_conflict = int((nuniq_nonblank >= 2).sum())
    mixed = int(g.apply(lambda s: s.notna().any() and s.isna().any()).sum())
    print(f"  race単位: 非空値ありrace率={float(has_nonblank.mean()):.4f}  全空race率={all_blank:.4f}")
    print(f"  非空値の真の競合race数={true_conflict:,}（0 なら 0.998 の不一致は空文字混在の見かけ）  "
          f"空/非空 混在race数={mixed:,}")


def _numeric_profile(df, cols, year):
    """列×年の total/finite/parse率、および数値列の min/median/max（分母効果と parse を可視化）。"""
    import numpy as np
    import pandas as pd
    years = sorted({int(y) for y in year.dropna().unique()})
    for c in cols:
        if c not in df.columns:
            print(f"  [{c}] 列なし")
            continue
        num = pd.to_numeric(df[c], errors="coerce")
        finite = num.notna()
        mn = float(np.nanmin(num)) if finite.any() else float("nan")
        md = float(np.nanmedian(num)) if finite.any() else float("nan")
        mx = float(np.nanmax(num)) if finite.any() else float("nan")
        print(f"  [{c}] parse率={float(finite.mean()):.3f} min/med/max={mn:.1f}/{md:.1f}/{mx:.1f}")
        cells = []
        for y in years:
            sub = (year == y).to_numpy()
            tot = int(sub.sum())
            fin = int((finite.to_numpy() & sub).sum())
            cells.append(f"{y}:{fin}/{tot}")
        print(f"        finite/total 年別: {'  '.join(cells)}")


def main() -> int:
    import pandas as pd

    from src.features._strictly_prior import has_leak, strictly_prior_runs

    print("=" * 88)
    print("H3 ソース契約 + coverage 監査 + リーク防止チェック（性能評価より前・contract 確定用）")
    print("=" * 88)

    # (1) 契約表（二時点: source発走時に既知 / target発走時に既知。H3 で使うのは後者）
    print("\n[1] ソース契約（二時点で分離。H3で重要なのは『target発走時に既知』の右列）")
    print(f"  {'仮説':<8}{'table':<14}{'column':<26}{'@source':<7}{'@target'}")
    for h, tbl, col, ksrc, ktgt, keys, sp, miss in CONTRACT:
        print(f"  {h:<8}{tbl:<14}{col:<26}{ksrc:<7}{ktgt}")
        print(f"  {'':8}{'':14}└ 結合={keys} / strictly-prior={sp} / 欠測={miss}")

    # (2) SED coverage（H3 の主ソース・年別 非欠測率）＝ ketto+race_id+ymd で全部揃うか
    print("\n[2] SED coverage 監査（H3 主ソース・ymd 年別 非欠測率）")
    sed = _load_jrdb("SED")
    if sed is None:
        print("  SED 未取得/未対応（ローカルで実行）。H3 は SED を主ソースにするため必須。")
    else:
        rid = "race_id" if "race_id" in sed.columns else ("race_key" if "race_key" in sed.columns else None)
        ymd = "ymd" if "ymd" in sed.columns else _date_col(sed)
        print(f"  SED 行数={len(sed):,}  race_id列={rid}  日付列={ymd}  ketto列={'あり' if 'ketto' in sed.columns else 'なし'}")
        if ymd is not None:
            year = _year_series(sed, ymd)
            _coverage_table(sed, [c for c in SED_H3_COLS if c in sed.columns], year)
            # 馬ID(ketto)同定統計（サンプル馬0頭問題の実データ解決）
            print("\n  [SED ketto 同定統計]")
            if "ketto" in sed.columns:
                _identity_stats(sed, "ketto", ymd)
            # H3b 上り/前後3F の数値 parse・妥当範囲・分母（率だけで判断しない）
            print("\n  [SED 数値 parse/range（H3b の上り/前後3F・分母効果の可視化）]")
            _numeric_profile(sed, [c for c in ("ato3f_time", "mae3f_time", "agari_idx", "ten_idx")
                                   if c in sed.columns], year)
            # H3a 過去走zラベル(race_pace/uma_pace) の state 変換成功率
            for zc in ("race_pace", "uma_pace"):
                if zc in sed.columns:
                    v = sed[zc].astype(str).str.strip().str.upper()
                    ok = float(v.isin(["H", "M", "S"]).mean())
                    print(f"  [{zc}] H/M/S 変換成功率={ok:.3f}  値分布={dict(list(v.value_counts().items())[:6])}")
        else:
            print("  [NG] SED に日付列(ymd)が無い＝strictly-prior 順序付け不能。")

    # (2b) KYI pace_yosou（H3a の発走前予想）
    print("\n[2b] KYI pace_yosou（H3a の発走前 P_r(z)・前日ファイル＝発走前）")
    kyi = _load_jrdb("KYI")
    if kyi is None:
        print("  KYI 未取得/未対応。H3a の P_r(z) ソースが無い＝現状 H3a は SOURCE_MISSING。")
    else:
        has_py = "pace_yosou" in kyi.columns
        print(f"  KYI 行数={len(kyi):,}  pace_yosou列={'あり' if has_py else 'なし'}  "
              f"ketto列={'あり' if 'ketto' in kyi.columns else 'なし'}")
        if has_py and "race_id" in kyi.columns:
            vc = kyi["pace_yosou"].astype(str).str.strip().value_counts(dropna=False)
            print(f"  pace_yosou 値分布(上位): {dict(list(vc.items())[:6])}")
            # 空文字を欠測化してから race 単位 coverage と真の競合を精査（見かけの不一致を排除）
            _pace_yosou_race_coverage(kyi)

    # (2c) KYI↔SED の ketto+race_id 結合カバレッジ（H3a は現レースKYI予想×過去SED実績を ketto で束ねる）
    if sed is not None and kyi is not None and "ketto" in sed.columns and "ketto" in kyi.columns:
        sed_ketto = set(sed["ketto"].astype(str))
        kyi_ketto = set(kyi["ketto"].astype(str))
        inter = sed_ketto & kyi_ketto
        print(f"\n[2c] ketto 結合: SED馬={len(sed_ketto):,} KYI馬={len(kyi_ketto):,} "
              f"共通={len(inter):,}（KYI予想×SED実績を ketto で束ねられる割合の目安）")

    # (3) リーク防止 機械チェック（SED を ketto でまとめ ymd で strictly-prior）
    print("\n[3] リーク防止 機械チェック（SED ketto 履歴・ymd で未来/同レースを除外できるか）")
    if sed is not None and "ketto" in sed.columns:
        rid = "race_id" if "race_id" in sed.columns else ("race_key" if "race_key" in sed.columns else None)
        ymd = "ymd" if "ymd" in sed.columns else _date_col(sed)
        if ymd is None or rid is None:
            print(f"  スキップ（ymd={ymd} / race_id={rid} が無い）。")
        else:
            g = sed[[ "ketto", ymd, rid]].copy()
            g["_d"] = pd.to_datetime(g[ymd], errors="coerce", format="%Y%m%d") \
                if g[ymd].astype(str).str.len().median() == 8 else pd.to_datetime(g[ymd], errors="coerce")
            counts = g.groupby("ketto").size()
            sample = list(counts[counts >= 2].index[:300])
            ok = checked = 0
            for k in sample:
                runs = g[g["ketto"] == k]
                d = runs["_d"]
                if d.notna().sum() < 2:
                    continue
                tgt_date = d.max()
                tgt_rid = runs.loc[d.idxmax(), rid]
                prior = strictly_prior_runs(runs, tgt_date, target_race_id=tgt_rid,
                                            date_col="_d", race_id_col=rid)
                checked += 1
                if not has_leak(prior, tgt_date, target_race_id=tgt_rid,
                                date_col="_d", race_id_col=rid):
                    ok += 1
            print(f"  サンプル馬(ketto) {checked} 頭で strictly-prior 検証: リーク無し {ok}/{checked}")
            print(f"  → {'PASS' if ok == checked and checked > 0 else 'FAIL/要確認'}"
                  f"（最新走を target にした時に集約へ混入しないこと・ketto+ymd+race_id で厳密）")
    else:
        print("  SED/ketto が無いためスキップ。")

    # (3b) target(featured/results)↔SED の (race_id,馬番) 一致率（H3 特徴を target に載せられるか）
    print("\n[3b] target↔SED 結合率（featured の (race_id,馬番) が SED に在るか）")
    if sed is None:
        print("  SED が無いためスキップ。")
    else:
        try:
            from app._model_eval import load_featured_data
            feat = load_featured_data()
        except Exception:  # noqa: BLE001
            feat = None
        rid = "race_id" if "race_id" in sed.columns else ("race_key" if "race_key" in sed.columns else None)
        if feat is not None and rid is not None and "umaban" in sed.columns:
            import pandas as pd
            fr = pd.DataFrame({"race_id": feat.index.astype(str),
                               "umaban": pd.to_numeric(feat.get("馬番"), errors="coerce")})
            skey = set(zip(sed[rid].astype(str), pd.to_numeric(sed["umaban"], errors="coerce")))
            m = fr.apply(lambda r: (r["race_id"], r["umaban"]) in skey, axis=1)
            print(f"  featured {len(fr):,} 行のうち SED に (race_id,馬番) 一致={float(m.mean()):.4f}"
                  f"（H3 特徴を target 側へ載せられる割合）")
        else:
            print("  featured 無し or SED に race_id/umaban 無しでスキップ。")

    # (4) 参考: horse_results 構造（H3 では不使用だが、race_id 欠如を明示記録）
    print("\n[4] 参考: horse_results 構造（H3 では不使用・race_id 欠如の記録）")
    hr, hr_path = _load_horse_results()
    if hr is None:
        print("  horse_results 読めません。")
    else:
        hid_col = "horse_id" if "horse_id" in hr.columns else (hr.index.name or "index(無名)")
        print(f"  source={hr_path} 行数={len(hr):,} 列数={len(hr.columns)} horse_id列={'horse_id' if 'horse_id' in hr.columns else 'なし(index?)'} "
              f"race_id列={'あり' if 'race_id' in hr.columns else 'なし'}")
        print(f"  列: {list(hr.columns)[:30]}")

    print("\n[結論] この契約表 + SED/KYI coverage + ketto リークチェックが揃ってから H3a/H3b を freeze。")
    print("  性能（ΔNLL）は次工程。ここで pace_yosou が SOURCE_MISSING なら先に KYI 取込を実施。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
