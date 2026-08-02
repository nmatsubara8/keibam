# 時系列分割 standing protocol（新仮説の検証規律）

予測系研究（NLL 上の直交情報の検証）で新仮説を検証する際の標準規律。**holdout は「一度も見る前に予約した」
場合のみ有効**。重要なのは **「証拠として何に使えるか」と「学習(refit)に使えるか」は別軸** であること。

## 区分（証拠状態）と学習可否（別軸）

| 区分 | 年（既定） | 評価/選択/独立証拠 | 最終 refit(係数学習) |
|---|---|---|---|
| excluded | 〜2014（stub） | 不可 | 不可 |
| **development_known** | 2015–2024 | 探索・**内部選択**は可。ただし**独立一般化証拠にはしない** | 可 |
| **burned_for_evidence** | 2025–（観測済） | **不可**（評価/選択/test に使わない） | **可**（test 前なら refit に使える） |
| **reserved_test** | freeze 時点で未観測の明示窓（当面 2027+） | **最終テスト（一度だけ）** | 不可 |
| **consumed_test** | 一度評価した test | 以後 **burned** 扱い | 可（次仮説の refit には可） |

- **refit（frozen 仕様の最終係数学習）は test より前の全データ**（development_known＋burned）を使える。
  burned を学習から外す必要はない。学習可否 = `refit_allowed(year, test_start) = 2015<=year<test_start`。
- **選択（特徴/L2/前処理を決める評価）は development_known(2015-2024)のみ**。burned/test での選択は
  researcher 過適合/汚染。`selection_allowed(year)` / `assert_selection_only_on_known(...)`。

## 3つの訂正（続29・ユーザ査読）

1. **burned は「学習禁止」でなく「証拠再利用禁止」**。2025-2026 は評価/選択/独立再現には使えないが、
   仕様を完全 freeze した後の最終 refit には使える。→ B の 2027 確認は **最終 fit=2015-2026 / test=2027** でよい
   （2025-2026 を fit から外さない）。
2. **2023-2024 は clean validation ではない**。P1/A/B/P2/H3 で観測済。内部選択には使えるが独立一般化証拠に
   しない。よって 2015-2024 を一括 `development_known`。複数仮説を同じ 2023-2024 で選び続けると validation
   過適合に注意。
3. **reserved_test は永久 holdout でない**。一度評価すると `reserved → evaluated → consumed`。B が 2027 を
   評価したら 2027 は consumed＝以後 burned。次の新仮説の test は 2028+（未接触窓）にずらす。**同じ 2027 を
   複数仮説に使うなら、2027 を見る前に全仮説を一括事前登録**する（後から足した仮説には clean でない）。

## 日付粒度（year では表せない prospective）

freeze 時点の **data cutoff 以降に新規に走った**レースは、同一暦年でも未接触＝clean test に回収できる
（2026 後半など）。year 区分では表せないため date 粒度で固定・assert する:

```
freeze_timestamp / data_cutoff(race or date) / test_start_date / test_end_date / interim_looks=0
assert: test race date > frozen data cutoff   （assert_test_after_cutoff）
```

`scripts/run_residual_head_prospective.py`（層2・freeze_date=2026-08-02・trigger min_test_races=5000）が担う。
`scripts/run_residual_head_2027.py`（層3・2027 カレンダー）は year 粒度で `assert_clean_final_test` により保護。

## assert が持つべき条件（日付＋状態）

年ベース assert に加え、最終テスト直前に:

```
freeze_timestamp < test_start_timestamp
max_training_date < min_test_date
test tranche status == reserved      （consumed でない）
test data hash が未評価             （manifest の generated_feature_hash 一致）
interim_look_count == 0
```

**コードは「研究者が 2027 を見た事実」までは自動判定できない**ため、reserved/consumed の状態は ledger／
manifest に保存する運用規約とする（`reserved_test_start` と `consumed_test_years` を manifest に記録）。

## 実装

`src/training/_temporal_split.py`（純関数）:
- `phase_of(year, *, reserved_test_start=2027, consumed_test_years=())`
- `refit_allowed(year, test_start)` / `selection_allowed(year)` / `phase_counts(...)`
- `assert_clean_final_test(train_years, test_years, *, reserved_test_start, consumed_test_years)`
  — **train は burned を含んでよい**・test は reserved 窓のみ・consumed 再利用不可・時系列
- `assert_selection_only_on_known(selection_years)` / `assert_test_after_cutoff(cutoff, test_dates)`

## B の具体形

```
仕様選択・既存証拠: 2015–2026（既知・独立証拠にはしない）
最終 refit:        test 開始より前の全データ（2015–2026・burned 含む）／特徴5列・L2=1.0・前処理 完全固定
clean test:        事前予約した未観測窓＝2026 後半の固定 tranche(prospective) または 2027 通年
評価後:            その窓を consumed_test へ移す（以後 burned・次仮説は次の未接触窓）
```

## 新仮説の手順（今後）

1. **development_known(2015–2024)** で探索＋特徴/L2/前処理を選択（`assert_selection_only_on_known`）。
2. 仕様を **freeze**（以後変えない）。
3. 最終 refit は **test 開始前の全データ**（burned 含む）。
4. **reserved_test（未接触窓）** で `assert_clean_final_test`（＋日付/状態 assert）を通し **一度だけ** 評価。
5. 判定は事前規則（🟢 ΔNLL≤−MES & CI上限<0 & ΔECE≤+tol ／ 🟡 CI上限<0 & sub-MES ／ ❌ それ以外）。
   🟢 でも NLL 上の候補であり ROI/控除超過/サイジングは別の新仮説。
6. 評価後、その窓を **consumed** に記録。次仮説は次の未接触窓へ。

結果を見てから特徴/λ/窓/前処理を同期間で再試行しない（多重探索）。単位: ΔNLL は自然対数＝**nats/race**
（CMI のみ log2=bit）。
