"""時系列 3 分割の standing protocol（dev / val / 未接触 final-test）＋混同防止 assert。

**背景**: holdout は「一度も見る前に予約した」場合のみ有効。本リポジトリの研究フェーズ(P1/A/B/P2/H3)は
2018-2026 を rolling-origin で横断済みゆえ、**2025-2026 は既に観測済（burned）＝clean final-test に使えない**。
唯一未接触なのは 2027+。そこで**今後の新仮説**は必ず次の年次規律で進める:

    dev   : 2015-2022  （特徴・状態・変換の探索）
    val   : 2023-2024  （特徴集合・L2・前処理・欠測処理をここまでで**freeze**）
    burned: 2025-2026  （既存仮説で観測済＝clean test 不可・新仮説の dev/val/test いずれにも使わない）
    test  : 2027+      （**未接触の最終テスト**・freeze 後に一度だけ）

**粒度の注記**: burned は本来「解析時にデータに存在した分」＝概ね現データ末尾まで。freeze 日以降に**新規に
走った**レースは同一暦年でも未接触なので、date 粒度の prospective 窓（run_residual_head_prospective）は
部分的に 2026 後半を clean test として回収できる。本モジュールは year 粒度の standing 規律を担い、date 粒度の
回収は prospective ハーネス側が担う。純関数のみ。
"""
from __future__ import annotations

from typing import Iterable

DEV_RANGE = (2015, 2022)
VAL_RANGE = (2023, 2024)
BURNED_RANGE = (2025, 2026)        # 既存仮説で観測済＝clean holdout 不可
UNTOUCHED_TEST_MIN = 2027          # 未接触 final-test 窓の開始（year 粒度）


def phase_of(year, *, untouched_test_min: int = UNTOUCHED_TEST_MIN) -> str:
    """年→フェーズ: 'excluded'(<2015 stub) / 'dev' / 'val' / 'burned'(2025-26) / 'test'(>=2027)。純関数。"""
    try:
        y = int(year)
    except (TypeError, ValueError):
        return "excluded"
    if y < DEV_RANGE[0]:
        return "excluded"          # pre-2015 は stub（DB完全性監査で確定）
    if DEV_RANGE[0] <= y <= DEV_RANGE[1]:
        return "dev"
    if VAL_RANGE[0] <= y <= VAL_RANGE[1]:
        return "val"
    if y >= untouched_test_min:
        return "test"
    return "burned"                # BURNED_RANGE（既観測・使わない）


def split_records(records, *, year_key="year", untouched_test_min: int = UNTOUCHED_TEST_MIN) -> dict:
    """records を phase 別に仕分ける。返す {'dev':[], 'val':[], 'burned':[], 'test':[], 'excluded':[]}。"""
    out: dict = {k: [] for k in ("dev", "val", "burned", "test", "excluded")}
    for r in records:
        out[phase_of(r.get(year_key), untouched_test_min=untouched_test_min)].append(r)
    return out


def phase_counts(years: Iterable, *, untouched_test_min: int = UNTOUCHED_TEST_MIN) -> dict:
    """年イテラブル→フェーズ別件数（監査表示用）。"""
    out: dict = {k: 0 for k in ("dev", "val", "burned", "test", "excluded")}
    for y in years:
        out[phase_of(y, untouched_test_min=untouched_test_min)] += 1
    return out


def assert_clean_final_test(train_years, test_years, *,
                            untouched_test_min: int = UNTOUCHED_TEST_MIN) -> bool:
    """最終テストの clean 性を強制（違反は ValueError）。新仮説の評価直前に必ず通す。

    (1) test は非空、(2) test は全て未接触窓(>=untouched_test_min)＝burned(2025-26)/dev/val を含まない、
    (3) train と test が年で重複しない、(4) max(train) < min(test)（時系列）。純関数（例外送出のみ）。
    """
    tr = sorted({int(y) for y in train_years})
    te = sorted({int(y) for y in test_years})
    if not te:
        raise ValueError("test_years が空。")
    burned_or_seen = [y for y in te if y < untouched_test_min]
    if burned_or_seen:
        raise ValueError(
            f"final-test に未接触窓外の年 {burned_or_seen} を含む（2025-2026 は既存仮説で観測済＝"
            f"clean holdout 不可・dev/val とも不可）。test は >= {untouched_test_min} のみ。")
    if set(tr) & set(te):
        raise ValueError(f"train と test が重複 {sorted(set(tr) & set(te))}。")
    if tr and max(tr) >= min(te):
        raise ValueError(f"時系列違反: max(train)={max(tr)} >= min(test)={min(te)}。")
    return True


def assert_freeze_before_test(freeze_years, test_years, *,
                              untouched_test_min: int = UNTOUCHED_TEST_MIN) -> bool:
    """特徴/L2/前処理を freeze した年群(dev+val)が全て test より前かを強制（val で freeze の規律）。"""
    fz = sorted({int(y) for y in freeze_years})
    te = sorted({int(y) for y in test_years})
    if fz and te and max(fz) >= min(te):
        raise ValueError(f"freeze 年 {max(fz)} が test {min(te)} 以降＝val までで freeze の規律違反。")
    # freeze は dev+val（<=2024）が原則。test 窓の年を freeze に使っていないこと。
    leaked = [y for y in fz if y >= untouched_test_min]
    if leaked:
        raise ValueError(f"freeze に未接触 test 窓の年 {leaked} を使用＝test 汚染。")
    return True
