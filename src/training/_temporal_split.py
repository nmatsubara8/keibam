"""時系列 standing protocol＝「証拠上の状態」と「学習可否」を分けた区分＋日付/状態 assert。

**要点（ユーザ査読・続29 で修正）**:
- `burned` は「学習禁止」ではなく **「証拠として再利用禁止」**。2025-2026 は評価/選択/独立再現には使えないが、
  仕様を完全 freeze した後の **最終 refit（係数学習）には使える**。→ 学習可否は別軸(`refit_allowed`)。
- 2023-2024 も P1/A/B/P2/H3 で観測済＝**clean validation ではない**。内部選択には使えるが独立一般化証拠に
  しない。よって 2015-2024 を一括 `development_known`（探索・内部選択・既知）とする。
- `test`(2027+) は **永久 holdout ではない**。一度評価すると consumed＝以後 burned。reserved→evaluated→
  consumed の状態遷移を持つ。同じ窓を複数仮説に使うなら**見る前に一括事前登録**が必要。

区分（証拠状態・phase_of）:
    excluded            : <2015（stub）
    development_known   : 2015-2024（探索・内部選択に使用可。独立証拠にはしない）
    burned_for_evidence : 2025..(reserved_test_start-1) の観測済（評価/選択/test 不可・refit は可）
    reserved_test       : >=reserved_test_start かつ未 consumed（freeze 時点で未観測の明示窓）
    consumed_test       : 既に一度評価した test（以後 burned 扱い）

学習可否（別軸）: refit_allowed(year, test_start) = 2015<=year<test_start（burned 含め test 前は全て可）。
選択可否: selection_allowed(year) = development_known のみ。純関数のみ。
"""
from __future__ import annotations

DEV_KNOWN_RANGE = (2015, 2024)     # 探索＋内部選択（既知・独立証拠にはしない）
BURNED_FROM = 2025                 # 2025〜 観測済（burned_for_evidence）
UNTOUCHED_TEST_MIN = 2027          # 既定の reserved_test 開始（2025-2026 は観測済ゆえ）


def phase_of(year, *, reserved_test_start: int = UNTOUCHED_TEST_MIN,
             consumed_test_years=()) -> str:
    """年→証拠状態。excluded/development_known/burned_for_evidence/reserved_test/consumed_test。純関数。

    学習可否は別軸（refit_allowed）。ここは「証拠として何に使えるか」の分類のみ。
    """
    try:
        y = int(year)
    except (TypeError, ValueError):
        return "excluded"
    if y in {int(c) for c in consumed_test_years}:
        return "consumed_test"
    if y < DEV_KNOWN_RANGE[0]:
        return "excluded"
    if DEV_KNOWN_RANGE[0] <= y <= DEV_KNOWN_RANGE[1]:
        return "development_known"
    if y >= reserved_test_start:
        return "reserved_test"
    return "burned_for_evidence"   # 2025..(reserved_test_start-1)


def refit_allowed(year, test_start_year) -> bool:
    """最終 refit（frozen 仕様の係数学習）に使えるか＝2015<=year<test_start（burned 含む）。純関数。"""
    try:
        y = int(year)
    except (TypeError, ValueError):
        return False
    return DEV_KNOWN_RANGE[0] <= y < int(test_start_year)


def selection_allowed(year) -> bool:
    """特徴/L2/前処理の**選択**に使えるか＝development_known(2015-2024)のみ。純関数。"""
    return phase_of(year) == "development_known"


def phase_counts(years, *, reserved_test_start: int = UNTOUCHED_TEST_MIN,
                 consumed_test_years=()) -> dict:
    out = {k: 0 for k in ("excluded", "development_known", "burned_for_evidence",
                          "reserved_test", "consumed_test")}
    for y in years:
        out[phase_of(y, reserved_test_start=reserved_test_start,
                     consumed_test_years=consumed_test_years)] += 1
    return out


def assert_selection_only_on_known(selection_years) -> bool:
    """特徴/L2/前処理の選択に burned/test 年を使っていないことを強制（development_known のみ許可）。"""
    bad = [int(y) for y in selection_years if not selection_allowed(y)]
    if bad:
        raise ValueError(f"選択に development_known(2015-2024) 外の年 {sorted(set(bad))} を使用"
                         "（burned/test での選択は researcher 過適合/汚染）。")
    return True


def assert_clean_final_test(train_years, test_years, *,
                            reserved_test_start: int = UNTOUCHED_TEST_MIN,
                            consumed_test_years=()) -> bool:
    """最終テストの clean 性を強制（違反は ValueError）。**train は burned を含んでよい（refit 可）**。

    (1) test 非空、(2) test は全て reserved_test_start 以上（burned/dev/val を test にしない）、
    (3) test に consumed 年を含まない（使い回し禁止）、(4) max(train)<min(test)（時系列・train は
    2025-2026 を含めた refit 可）。純関数（例外送出のみ）。
    """
    tr = sorted({int(y) for y in train_years})
    te = sorted({int(y) for y in test_years})
    consumed = {int(c) for c in consumed_test_years}
    if not te:
        raise ValueError("test_years が空。")
    too_early = [y for y in te if y < reserved_test_start]
    if too_early:
        raise ValueError(f"test に reserved 窓(>= {reserved_test_start})外の年 {too_early}"
                         "（2025-2026 は観測済＝clean test 不可・dev/val も不可）。")
    reused = [y for y in te if y in consumed]
    if reused:
        raise ValueError(f"test に consumed 済みの年 {reused}（一度使った窓は再利用不可＝以後 burned）。")
    if tr and max(tr) >= min(te):
        raise ValueError(f"時系列違反: max(train)={max(tr)} >= min(test)={min(te)}。")
    return True


def assert_test_after_cutoff(data_cutoff_date, test_dates) -> bool:
    """日付粒度: 全 test レース日が frozen data cutoff より後（prospective の clean 性）。違反は ValueError。

    freeze 時点の data cutoff 以降に**新規に走った**レースのみ test にできる（2026 後半の回収など・年単位
    では表せない）。data_cutoff_date/test_dates は pandas 変換可能な日付。純関数（例外送出のみ）。
    """
    import pandas as pd
    cutoff = pd.to_datetime(data_cutoff_date)
    d = pd.to_datetime(pd.Series(list(test_dates)), errors="coerce")
    if d.isna().any():
        raise ValueError("test_dates に解釈不能な日付。")
    if (d <= cutoff).any():
        n = int((d <= cutoff).sum())
        raise ValueError(f"test に data cutoff({cutoff.date()}) 以前のレースが {n} 件"
                         "（freeze 後に新規に走った分のみ prospective test 可）。")
    return True
