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
CONTRACT = [
    # (hypothesis, source_table, source_column, pre_race, join_keys, strictly_prior, missing)
    ("H3a", "KYI(前日)", "pace_yosou(H/M/S)", "YES=前日ファイルのレースペース予想（発走前）",
     "(race_id,馬番)→race単位で一定", "現レースの予想は当該レースのみ・過去の予想は使わない",
     "未取得→P_r(z) 欠測→適性NaN(安全)"),
    ("H3a", "horse_results", "ペース(前半-後半3F)", "NO=過去走の実測（過去の事実として利用可）",
     "horse_id", "date<target_date & race_id≠target", "解釈不能→その走を除外"),
    ("H3a", "horse_results", "人気/着順/頭数", "NO=過去走の確定結果（市場超過残差の材料）",
     "horse_id", "date<target_date & race_id≠target", "欠損走は残差から除外"),
    ("H3a(参考)", "SED", "race_pace(H/M/S 実測)", "NO=レース後確定（過去走のzラベルにのみ）",
     "race_id", "過去走の race_id にのみ結合・現レースには使わない", "無→PACE文字列にフォールバック"),
    ("H3b", "horse_results", "上り(終い3F)", "NO=過去走の実測（終い速度の材料）",
     "horse_id", "date<target_date & race_id≠target", "欠損走は集約から除外"),
    ("H3b", "horse_results", "距離/馬場", "NO=過去走の条件（基準残差の層）",
     "horse_id", "date<target_date & race_id≠target", "層不明→全体基準へ縮約"),
    ("H3b(不使用)", "featured", "course_lap_length", "—（全欠測と判明・P2続15）",
     "—", "—", "使わない（レース定数でなく空列）"),
]


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
    """列×年の 非欠測率/unique を出す（raw 由来・結果不変の coverage 監査）。"""
    import pandas as pd
    years = sorted({int(y) for y in year.dropna().unique()})
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


def main() -> int:
    import pandas as pd

    from src.features._strictly_prior import has_leak, strictly_prior_runs

    print("=" * 88)
    print("H3 ソース契約 + coverage 監査 + リーク防止チェック（性能評価より前・contract 確定用）")
    print("=" * 88)

    # (1) 契約表
    print("\n[1] ソース契約（実データ検証の前提・freeze はこの表 + 下の coverage で確定）")
    print(f"  {'仮説':<9}{'table':<14}{'column':<22}{'発走前取得可否'}")
    for h, tbl, col, pre, keys, sp, miss in CONTRACT:
        print(f"  {h:<9}{tbl:<14}{col:<22}{pre}")
        print(f"  {'':9}{'':14}└ 結合={keys} / strictly-prior={sp} / 欠測={miss}")

    # (2) coverage 監査（horse_results）
    print("\n[2] coverage 監査（horse_results・年別 非欠測率）")
    hr, hr_path = _load_horse_results()
    if hr is None:
        print("  horse_results を読めません（ローカルで実行）。")
    else:
        dc = _date_col(hr)
        print(f"  source={hr_path}  行数={len(hr):,}  index名={hr.index.name}  日付列={dc}")
        if dc is None:
            print("  [NG] 日付列が無い＝strictly-prior 不能。列を確認。")
        else:
            year = _year_series(hr, dc)
            _coverage_table(hr, ["ペース", "上り", "人気", "着順", "頭数", "距離", "馬場", "通過"], year)

    # KYI pace_yosou（H3a の発走前予想）
    print("\n[2b] KYI pace_yosou（H3a の発走前 P_r(z)・前日ファイル＝発走前）")
    kyi = _load_jrdb("KYI")
    if kyi is None:
        print("  KYI 未取得/未対応。H3a の P_r(z) ソースが無い＝現状 H3a は SOURCE_MISSING。")
    else:
        has_py = "pace_yosou" in kyi.columns
        print(f"  KYI 行数={len(kyi):,}  pace_yosou 列={'あり' if has_py else 'なし'}")
        if has_py and "race_id" in kyi.columns:
            vc = kyi["pace_yosou"].astype(str).str.strip().value_counts(dropna=False)
            print(f"  pace_yosou 値分布(上位): {dict(list(vc.items())[:6])}")
            # race 単位で一定か（＝レースペース予想）を確認
            per_race_nuniq = kyi.groupby("race_id")["pace_yosou"].nunique()
            frac_const = float((per_race_nuniq <= 1).mean())
            print(f"  race 内 pace_yosou 一意率={frac_const:.3f}（1.0 に近い＝レース単位の予想＝P_r(z) 妥当）")

    # (3) リーク防止 機械チェック
    print("\n[3] リーク防止 機械チェック（strictly_prior_runs で未来/同レースを除外できるか）")
    if hr is not None and _date_col(hr) is not None:
        dc = _date_col(hr)
        rid_col = "race_id" if "race_id" in hr.columns else None
        # index=horse_id を想定。サンプル馬で「最新走を target にしたら strictly-prior から消える」検証
        sample_ids = list(pd.Index(hr.index.unique()))[:200]
        ok = 0
        checked = 0
        for hid in sample_ids:
            runs = hr.loc[[hid]] if hid in hr.index else None
            if runs is None or len(runs) < 2:
                continue
            d = pd.to_datetime(runs[dc], errors="coerce")
            if d.notna().sum() < 2:
                continue
            tgt_date = d.max()
            tgt_rid = runs.iloc[d.values.argmax()][rid_col] if rid_col else None
            prior = strictly_prior_runs(runs, tgt_date, target_race_id=tgt_rid,
                                        date_col=dc, race_id_col=rid_col or "race_id")
            checked += 1
            if not has_leak(prior, tgt_date, target_race_id=tgt_rid,
                            date_col=dc, race_id_col=rid_col or "race_id"):
                ok += 1
        print(f"  サンプル馬 {checked} 頭で strictly-prior 検証: リーク無し {ok}/{checked}")
        print(f"  → {'PASS' if ok == checked and checked > 0 else 'FAIL/要確認'}"
              f"（最新走を target にした時に集約へ混入しないこと）")
        if rid_col is None:
            print("  [注意] horse_results に race_id 列が無い＝target 自身の除外は日付のみに依存。"
                  "同日再走が無い前提（馬は1日1走）で安全だが、race_id 付与を推奨。")
    else:
        print("  horse_results が無い/日付列が無いためスキップ。")

    print("\n[結論] この契約表 + coverage + リークチェックが揃ってから H3a/H3b の特徴定義を freeze する。")
    print("  性能（ΔNLL）は次工程。ここで pace_yosou が SOURCE_MISSING なら先に KYI 取込を実施。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
