# 時系列分割 standing protocol（新仮説の検証規律）

本リポジトリの予測系研究（NLL 上の直交情報の検証）で、**新しい仮説を検証する際に必ず従う時系列分割**の
標準規律。目的は holdout の証拠力を守ること——**holdout は「一度も見る前に予約した」場合にのみ有効**。

## 分割（year 粒度）

| フェーズ | 年 | 用途 |
|---|---|---|
| excluded | 〜2014 | pre-2015 stub（DB完全性監査で確定・使わない） |
| **dev** | 2015–2022 | 特徴・状態・変換の探索 |
| **val** | 2023–2024 | 特徴集合・L2・前処理・欠測処理を**ここまでで freeze** |
| **burned** | 2025–2026 | 既存仮説(P1/A/B/P2/H3)で観測済＝**clean test に使えない**。新仮説の dev/val/test いずれにも使わない |
| **test** | 2027+ | **未接触の最終テスト**。freeze 後に**一度だけ**評価 |

実装: `src/training/_temporal_split.py`
- `phase_of(year)` → `excluded/dev/val/burned/test`
- `split_records(records)` / `phase_counts(years)`
- `assert_clean_final_test(train_years, test_years)` — test が未接触窓(>=2027)のみ・train と重複なし・
  max(train)<min(test) を強制（違反は `ValueError`）
- `assert_freeze_before_test(freeze_years, test_years)` — freeze(dev+val) が test より前・test 窓年を
  freeze に使っていないことを強制

## なぜ 2025–2026 は使えないか

研究フェーズ P1/A/B/P2/H3 はすべて 2018–2026 を rolling-origin で横断評価済み。特に B の rolling-origin は
2025・2026 を test fold として明示的に観測し（B の 2025:−0.00016 / 2026:+0.00040 等）、B の一部特徴
（`wet_rel_rank` 等）は 2015–2026 の分析からデータ駆動で選ばれた。ゆえに 2025–2026 を今「最終テスト」と
宣言しても**既に最適化・観測に使った期間での再評価＝循環**になり、out-of-sample の証拠力を持たない。

## 既存仮説（特に B）の最終テスト

2025–2026 が burned のため、既存仮説の唯一の未接触窓は **2027+**。加えて、**freeze 日以降に新規に走った
レースは同一暦年でも未接触**なので、date 粒度の prospective 窓で 2026 後半も clean test として回収できる。

- `scripts/run_residual_head_2027.py` — 層(3) 2027 カレンダー確認（`assert_clean_final_test` で保護）
- `scripts/run_residual_head_prospective.py` — 層(2) prospective shadow（freeze_date=2026-08-02 以降・
  trigger `min_test_races=5000`・date 粒度）

どちらも manifest-bound（feature/data/コードのハッシュ一致で `--evaluate`）・interim looks=0・m=1・
B=20,000/seed=0。特徴/L2/変換/欠測は凍結・再較正しない。

## 新仮説の手順（今後）

1. **dev(2015–2022)** で探索。
2. **val(2023–2024)** で特徴集合・L2・前処理・欠測処理を**確定(freeze)**。以後変えない。
3. `assert_freeze_before_test(freeze_years, test_years)` を通す。
4. **test(2027+)** で `assert_clean_final_test` を通し、**一度だけ**評価（interim looks=0）。
5. 判定は事前規則（🟢 ΔNLL≤−MES & CI上限<0 & ΔECE≤+tol ／ 🟡 CI上限<0 & sub-MES ／ ❌ それ以外）。
   🟢 でも NLL 上の候補であり ROI/控除超過/サイジングは別の新仮説。

**burned(2025–2026) を dev/val/test のいずれにも使わない。** 結果を見てから特徴/λ/窓/前処理を同期間で
再試行しない（多重探索）。単位: ΔNLL は自然対数＝nats/race（CMI のみ log2=bit）。
