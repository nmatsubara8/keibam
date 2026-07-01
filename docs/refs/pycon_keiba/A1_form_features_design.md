# Phase 2 / A1 form 集計特徴 — 設計と実装状況

PyCon2018 トークの A1（entity×context×集計）/ A2（スムージング）を keibam に落とす設計。
関連: `00_SYNTHESIS.md`、`.claude-context.md` §9。

## 既存カバレッジの棚卸し（重複回避のため先に把握）

keibam の特徴量パイプラインは既に成熟しており、**馬(horse_id)エンティティの時間前方集計**を
`src/preprocessing/_horse_features.py` が広く実装済み（すべて「date<当該レース」でリーク無し）:
- career（出走/勝利/勝率/賞金）、recent form 率（win/rentai/place/相対着順 × 5/9/20R）
- speed_figure（標準タイム偏差）集計、脚質(first_corner 由来)、growth_trend
- 適性（道悪/競馬場）、コース条件別（距離±100m/種別）、種別×馬場、クラス別、相手強度(faced grade)

日付カットは `_data_merger._merge_horse_results` が `searchsorted(date<target)` で厳密実施。
Elo（`_ratings.py`）と person 前年集計（`_merge_person_yearly`, year-1）もある。

**結論**: 「馬エンティティ × context」は既にほぼ A1 相当。今 form 特徴が 93–96% 欠損なのは、
これらの入力 `horse_results`（馬ページ）が 93–96% 未取得だから → **backfill 完了で自動的に埋まる**。

## A1 の真の空白（本タスクで先行実装）

未カバー or 粗いのは **「馬以外のエンティティ × context」の全履歴 expanding target-encoding**:
- 騎手/調教師/馬主: 既存は「前年集計」のみ。**全履歴 expanding・context 条件付き・スムージング無し**。
- 父(sire): peds join はあるが sire×context の target-encoding は未整備。

これらは **results 表（取得済み・backfill 不要）** の (race,horse) 行から計算できる。
→ **backfill を待たずに先行実装・検証できる**（本タスクの狙い）。

## 実装（済み・検証済み）

`src/preprocessing/_target_encoding.py`:
- `expanding_target_encode(df, keys, target, date_col, alpha, global_prior=None)`
  各行について **keys を共有する厳密過去（date<自分）** の target 平均をスムージングして返す。
  `smoothed = (Σ_past target + α·prior)/(n_past + α)`。prior 既定は「過去のみ expanding 全体平均」。
- `build_person_form_features(results, specs, alpha)`
  騎手/調教師/馬主 × context(race_type/開催) の勝率/複勝率 encoding をまとめて生成（列が無い spec は自動スキップ）。

### リーク回避の要（テスト済み）
`tests/preprocessing/test_target_encoding.py`（6件 PASS）で担保:
- **厳密過去**: date<自分のみ。**同一日の他行も除外**（同日・同レース内の複数騎乗リークを遮断）。
- **自分の結果は不参照**（最終行の着順を変えても encoding 不変）。
- **スムージング**: 高 α ほど少数カテゴリ(n小)を全体平均へ強く縮める。
- 履歴ゼロ行は全体 prior にフォールバック（NaN でなく安全既定）。context 追加で同 context の過去のみ集計。

規約は既存 horse_results の「date<target_date」と一致 → 学習/推論で同一計算・train/serve skew 無し。

## パイプラインへの結線（次ステップ・要実データ検証）

`_data_merger` に `_merge_person_yearly` と同様の薄い merge ステップを追加する想定:
1. `merge()` の `_merge_person_yearly` の後に `_merge_person_target_encoding` を追加。
2. results 履歴（当該 rebuild 対象の全 results）から `build_person_form_features` を呼び、
   `(race_id, 馬番)` 粒度で `self._results` に left-merge。
3. **ライブ推論**: shutuba（未来レース）は results に無いので、学習済み履歴の**最新スナップショット**で
   同じ encoding を引く関数を別途用意（Elo の `features_from_snapshot` と同型）。まず学習時特徴として
   入れ、backtest で効くか確認 → その後ライブ経路を整備、が安全。

### 検証プロトコル（結線後・実データ）
1. `rebuild-featured` → 新列の非null率と分布を確認。
2. **`python debug_leak.py`（単一特徴 AUC>0.9 検出）を必ず実行** — target-encoding は静かなリーク源。
   同日遮断はテスト済みだが、実データの date 欠損・重複 index で崩れないか最終確認。
3. `walk_forward.py --quality` / `stack_eval.py --years 2026` を再実行し、baseline(2026)から
   logloss/ECE 改善・echo 低下が出るか判定。

## パラメータ
- `alpha`（スムージング強度）: 既定 20。将来 A2 上級版として「目的変数との相互情報量で
  カテゴリ別最適 α」に拡張余地（PyCon）。まず単一 α で十分。
- context: race_type（芝/ダ）、開催（競馬場）を既定。距離帯/馬場は列整備後に追加。
