# JRDB42 予測価値 confirmation — 事前登録（B と joint・2027 tranche）

続36 で実体化認定した **JRDB 42-feature augment contract** の新規特徴が、市場（オッズ）に直交する
予測情報を持つかを confirmation する。**現時点（2026-08-02）では clean な数値は出せない**：2025-2026 は
burned、2027 は reserved で未発生、prospective shadow も min_test_races 未達。従って本書で仕様を **freeze
（凍結・事前登録）** し、未観測 tranche 到着時に **一度だけ** 評価する。B と 2027 を共有するため、
**両仮説をここで一括登録**する（後から足す仮説には 2027 は clean でない）。

## 仮説

- **JRDB42_RESIDUAL_2027_CONFIRM**: market-anchored residual head `P=softmax(log q + θ·x)` に、実体化した
  JRDB 特徴 x（下記 41 列）を入れると、市場のみ（θ≡0）に対し race-weighted 平均 ΔNLL が負に有意。
- 帰無: ΔNLL ≥ 0（新規特徴は市場に直交する追加情報を持たない）。

## 凍結仕様（結果を見て変えない）

| 項目 | 値 |
|---|---|
| model | market-anchored residual head（B と同一コア `fit_and_eval`/`fit_residual_head`） |
| features | **41 列** = CURRENT_ACTIVE_JRDB(33) ＋ HISTORY_JRDB(8)。CONTEXT(jrdb_pace_hms)は within-race softmax で相殺のため**除外** |
| L2 | 1.0（B と同 regime）※ dev-CV 選定するなら freeze 前に置換・manifest 記録 |
| preprocessing | within-race z-score（`build_residual_records`・B と同一） |
| estimand | race-weighted 平均 ΔNLL（自然対数＝**nats/race**） |
| MES | 0.001 nats/race |
| ECE 許容 | +0.005 |
| bootstrap | venue×日 block（race_id[:10]）・B=20,000・seed=0・centered one-sided ASL |
| test | reserved_test=2027（JRA限定・NAR=0）・**一度だけ**・interim looks=0 |
| 多重性 | **Holm**（family={B_RESIDUAL_HEAD_2027_CONFIRM, JRDB42_RESIDUAL_2027_CONFIRM}・m=2） |
| leak 安全 | history 列の strictly-prior 性は build 時（`jrdb_build_features.py` の leak manifest）で fail-closed 認定済み前提 |
| 学習側 | 明示 allowlist（`assert_no_unguarded_augment` を満たす）＝新規列 silent 混入なし |

## 判定規則（事前・`verdict`）

- 🟢 Confirmed: 95%CI 上限 < 0 かつ ΔNLL ≤ −MES かつ ΔECE ≤ +0.005（**Holm 適用後**に有意）。
- 🟡 Replicated sub-MES: CI 上限 < 0 だが −MES < ΔNLL < 0。
- ❌ Not confirmed: それ以外。
- family の最終判定は B と JRDB42 の primary p 値に Holm（m=2）を適用（`holm_reject`）。

## 実行（manifest-bound audit → evaluate）

```
# 1. 完全 augment featured を build（strictly-prior manifest を fail-closed で確認）
python scripts/jrdb_build_features.py --jrdb-dir <dir> --with-myspeed --out data/featured_jrdb.pkl
# 2. 監査（性能を見ない・2027 完全性ゲート＋freeze commit を manifest へ）
python scripts/run_jrdb42_confirm.py --audit-only --featured data/featured_jrdb.pkl \
    --manifest-out artifacts/jrdb42_2027_audit.json
# 3. 2027 全開催終了後に一度だけ（manifest 一致検証後に ΔNLL）
python scripts/run_jrdb42_confirm.py --evaluate --featured data/featured_jrdb.pkl \
    --audit-manifest artifacts/jrdb42_2027_audit.json
```

## freeze 決定（2026-08-02・ユーザ確定済み）

1. **features = 41 列すべて**（CURRENT_ACTIVE 33＋HISTORY 8）で確定。部分集合にはしない
   （L2 が collinearity を吸収するため、市場直交情報の有無を最も広く検定する）。仮説は1本（family m=2）。
2. **L2 = 1.0 に事前コミット**で確定（B と同 regime・結果を見ずに固定・researcher 自由度ゼロ）。
   development CV による L2 選定は**行わない**。

この2点は結果を見ずに確定済み。最初の `--audit-only` を実行した時点の commit を `freeze_commit` として
manifest に刻む（＝凍結の時刻印）。以後は特徴集合・L2・前処理・判定規則を変えない。ハーネス
`scripts/run_jrdb42_confirm.py` の `FROZEN` は既にこの決定（features=41・l2=1.0）と一致している。

### 注記: jrdb_kokyu_flag は TEMPORALLY_DEAD だが 41 に残す（inert）

`diagnose_feature.py` で確定: jrdb_kokyu_flag は 2015-2018 に変動（{0,1,2}・race内分散≈0.34）するが
**2020 以降は全行 0（saved/fresh parity=1.0＝データ自体が定数化）**。test(2027) 期は定数ゆえ market
residual head では within-race 相殺で **θ 寄与≈0＝inert**（リーク/有害でない）。データを見て凍結列を
削ると selection 汚染になるため、**41 のまま維持**する（`TEMPORALLY_DEAD_JRDB` に登録し監査は既知扱い）。

## 現時点で clean 数値が出ない理由（明示）

- 2025-2026 = burned（評価に使うと証拠が焼ける）。2027 = reserved・未発生。prospective(2026後半) = 蓄積中。
- 従って「今すぐ数値」は **in-sample / development(2015-2024) 内部 CV の参考値**（独立証拠でない）としてのみ
  可能。参考ハーネス `scripts/run_jrdb42_insample_reference.py` を用意（rolling-origin・train=[2015,eval)・
  test=eval・2025+ は `assert_selection_only_on_known` で fail-closed・features=41/l2=1.0 と凍結一致）:

  ```
  python scripts/run_jrdb42_insample_reference.py --featured data/featured_jrdb.pkl
  ```

  出力は fold 別 ΔNLL＋全 fold プールの block-bootstrap CI（nats/race）。**⚠非証拠**（selection 域・
  過適合を含みうる）。ここで良く見えても freeze 仕様（特徴/L2/判定規則）は変えない。採否は 2027 で一度だけ。
