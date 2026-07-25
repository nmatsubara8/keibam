# 予想ファクター拡張計画（未実装ファクターの取り込み）

作成: 2026-07-24。競馬予想の主要ファクター一覧と現行実装を突き合わせ、
未実装ファクターの取り込みを 6 フェーズで計画する。

スコープ決定（ユーザー確認済み）:
- **グループA（追加スクレイピング不要）+ グループB（netkeiba 無料範囲のみ）** を対象
- netkeiba 有料会員ではない → 会員限定コンテンツ（調教詳細・厩舎コメント等）は対象外
- グループC（外部気象データ: 風向・風速・気温）は今回対象外

---

## 1. 現状調査サマリ

### 実装済みファクター
基本属性（体重/体重変化/年齢/性/斤量/枠番/馬番/頭数/interval/age_days）、
コース・馬場・天候・クラスのダミー化、馬の過去着順の多窓集計
（5/9/20R+全体 × mean/std/max/min/median）、騎手・調教師・種牡馬・コース別の
勝率/平均着順、脚質（pace_median / leg_type_binary / pace_at_distance）、
血統 62 列ラベルエンコード、交互作用（枠×コース種別等）、レース内 Z-score。

### 未実装だが raw データは収集済み（死蔵）
- `HorseResultsProcessor` が `time_seconds` / `上り` / `first_corner` 等の通過順派生 /
  `ペース` / `オッズ` / `人気` を計算済みだが、集計対象が `target_cols=["着順"]` のみの
  ため特徴量化されていない（実質デッドカラム）
- `horse_info.pkl` に owner_id / breeder_id / 通算成績列あり（成績集計は未実装）
- 過去成績の `騎手` 列あり（乗り替わり判定に使えるが未使用）

### 未収集（スクレイピング拡張が必要）
- 調教タイム/追い切り（netkeiba 調教ページ。**会員限定の可能性が高い**）
- 入厩日・厩舎コメント（会員限定列 → 対象外）
- 出馬表ページの脚質マーク等の無料枠情報
- 風速・風向・気温（netkeiba に項目なし → グループC、対象外）

### 既知のサイレントバグ（本計画で是正）
1. `group_cols=["騎手"]` は `ResultsProcessor` が騎手列を drop しているため
   一度も発動していない（`_merge_aggregates` が `if group_col not in columns: continue`）
2. ライブ推論時、`ShutubaDataMerger` の `self._results` は出馬表のみで過去履歴が
   空のため `jockey_win_rate` 等が全 NaN → `feature_names_` の reindex fill 0 頼み
   （Phase 5 の entity-stats artifact パターンで是正）

---

## 2. 追加するファクター（12 項目）

| # | ファクター | グループ | フェーズ |
|---|---|---|---|
| 1 | 過去走タイム・上がり3F・通過順（展開）の多窓集計 | A | 1 |
| 2 | スピード指数（IDM 相当、基準タイム標準化で内製） | A | 3 |
| 3 | 馬自身の通算出走回数・勝率・連対率・複勝率 | A | 1 |
| 4 | 馬主・生産者の成績集計 | A | 5 |
| 5 | 体重増減率 | A | 2 |
| 6 | ローテーションカテゴリ（連闘/中1-2週/…/休養明け） | A | 2 |
| 7 | 乗り替わり・テン乗りフラグ | A | 2 |
| 8 | レース展開予測（出走馬の脚質分布・想定ペース） | A | 4 |
| 9 | 種牡馬の距離帯別適性 | A | 4 |
| 10 | 過去n走のオッズ・人気の集計（過去走由来でリークなし） | A | 1 |
| 11 | 調教タイム/追い切り（無料範囲があれば） | B | 6 |
| 12 | 出馬表の脚質マーク等・無料枠 | B | 6 |

---

## 3. フェーズ別実装計画

### Phase 0: 基盤整備（全フェーズの前提）— ✅ 完了（2026-07-24）

- `src/constants/_feature_cols.py` に `AGG_TARGET_COLS = ["着順"]` /
  `AGG_GROUP_COLS = ["騎手"]` を新設し、`run_pipeline.py`（builder 2 箇所）・
  `tests/integration/test_live_prediction.py`・`main.ipynb`（cell 29）のリテラルを
  定数参照化（学習/ライブのパリティ事故の温床を解消）
  - 発見: `main.ipynb`（ライブ推論経路）は `run_pipeline`（学習）と**異なる**
    target_cols（着順以外に prize/corner/time_seconds 等の広い集合）を使っており、
    潜在的パリティ不整合があった。Phase 0 で全経路を単一定数へ収束（現状は production
    値 `["着順"]`/`["騎手"]`。notebook の旧・広集合は cell 29 のコメントに退避し、
    Phase 1 で `AGG_TARGET_COLS` に段階的に取り込む）
- `test_train_live_feature_parity` を追加: 学習 featured_data とライブ featured の
  **構造的列**（One-Hot ダミー "__" を除く）の集合一致を assert し、`feature_names_`
  reindex fill 0 が握り潰す前に差分を可視化
- **検証結果**: pytest 738 passed（core）+ 133 passed（training）/ 全 skip は
  torch・ローカルデータ非依存分のみ。ruff clean・import-linter 4 契約 KEPT
- **未実施（データ環境が必要）**: retrain でのベースライン AUC（0.772〜0.774）+
  feature importance の記録。Phase 0 は現 production と挙動同値（run_pipeline の
  定数値は不変）のため refactor としては安全。ベースライン記録は実データ環境
  （VPS 等）で `retrain` 実行時に取得し、以後のフェーズの比較基準とする

### Phase 1: target_cols 拡張 + 通算成績（項目 1, 3, 10）— ✅ 完了（2026-07-24）

**実装サマリ**:
- `AGG_TARGET_COLS` を 8 列へ拡張（着順 + time_seconds / 上り / first_to_rank /
  final_to_rank / first_to_final / オッズ / 人気）。多窓集計と Z-score は既存機構が
  自動生成（列名規約・Z-score リストとも無変更で波及）
- `HorseResultsProcessor._preprocess` に オッズ / 人気 / 上り の `to_numeric(coerce)` を追加。
  `_data_cleaner` の `_horse_results` 辞書から 人気 の float→int(fillna 0) を除去し、
  欠損人気が平均を汚染しないよう NaN のまま集計除外
- `DataMerger._add_horse_career_stats()` を per-date ループの集計直後に新設
  （n_career_starts / career_win_rate / career_quinella_rate / career_place_rate）。
  `HORSE_CAREER_FEATURE_COLS` を Z-score named 群へ合流
- ライブ側は `_merge_horse_results` 継承により自動でパリティ成立（追加配線なし）

**検証結果**:
- unit: `tests/preprocessing/test_data_merger_features.py` に拡張集計・通算成績・NaN 除外・
  リーク境界（事前フィルタ空→NaN）のテストを追加 → 26 passed
- 合成データ E2E スモーク: merge → Z-score で `オッズ_mean_5R` / `career_win_rate_z` 等
  181 個の `_z` 列を含む全列が生成されることを確認
- 既存 pytest 196 passed / 8 skip、mypy clean、ruff（src）clean、import-linter 4 契約 KEPT
- **リーク**: 構造的に過去走（date < 当日）のみ参照。当該レースの 上り/タイム/オッズ/人気は
  ResultsProcessor で未選別のまま維持（唯一のリーク境界）。ラベルシャッフル検査
  `scripts/leakage_check.py` は実データ環境で実施予定

**留意（フォローアップ候補）**:
- 列数が約 +360（8 集計列 × 5 統計 × 4 窓 × 2 の _z）増加。`add_race_level_zscore` が
  列逐次 insert のため DataFrame fragmentation の PerformanceWarning が出る（機能影響なし・
  既存パターンの増幅）。AUC 比較後に importance 下位統計量のサブセット化 or z-score の
  一括 concat 化を検討

- `AGG_TARGET_COLS` を拡張:
  `["着順", "time_seconds", "上り", "first_to_final", "final_to_rank", "first_to_rank", "オッズ", "人気"]`
  - `first_corner` 絶対値は頭数依存 → `HorseResultsProcessor._preprocess` に
    `first_corner_rate = first_corner / 頭数`（`final_corner_rate` 同様）を追加して集計
  - `オッズ`/`人気`/`上り` に `pd.to_numeric(errors="coerce")`（"---" 等の混入対策）。
    `_data_cleaner.py` の型辞書への波及を確認
- 波及なしを確認済みの点:
  - 列名規約 `{col}_{stat}_{n}R` は `_summarize` が自動生成 → 変更不要
  - Z-score は `endswith("_{stat}_{n}R")` の動的検出 → 自動で `_z` 化、リスト変更不要
  - `ShutubaDataMerger` は `_merge_aggregates` を継承 → 定数共有だけでパリティ成立
  - 新列はすべて過去走由来 → DataSplitter の drop 追加不要。
    `ResultsProcessor._select_columns` は当該レースの上り/タイム/人気を現状どおり不採用
    のまま維持（これが唯一のリーク境界）
- 通算成績: `DataMerger._add_horse_career_stats()` を per-date ループの
  `_merge_aggregates` 直後に新設。`n_career_starts` / `career_win_rate` /
  `career_quinella_rate` / `career_place_rate`。着順 NaN 行（中止・除外）drop 済みの
  ため完走ベースであることを docstring に明記
- 列数増（8 列 × 5 統計 × 4 窓 × 2 で +320 程度）→ AUC 比較後に importance 下位の
  統計量をサブセット化する余地を残す

### Phase 2: FE 系 + 乗り替わり（項目 5, 6, 7）— ✅ 完了（2026-07-24）

**実装サマリ**:
- `FeatureEngineering.add_weight_change_rate()`: `体重変化 / 体重`（体重0は NaN、float64 維持）。
  `RACE_LEVEL_ZSCORE_COLS_G1` に `weight_change_rate` を追加（Z-score 対象）
- `FeatureEngineering.add_rotation_category()`: `interval` を `ROTATION_BINS`
  `[0,7,14,28,56,inf]` で 5 区分（連闘/中1-2週/中3-8週/中5-8週/休養明け）に `pd.cut` →
  既存 `_dummify` で固定カテゴリ One-Hot 化。初出走（interval NaN）は `rotation_first_run` フラグ
- 乗り替わり: `DataMerger._add_jockey_change()` + `_normalize_jockey()`（見習マーク☆▲△等を
  除去して比較）。前走騎手比較で `jockey_change`、騎乗歴なしで `first_ride`（テン乗り）。
  判定用の `jockey_name` は使用後 drop（生の騎手名は学習に渡らない・二値のため Z-score 対象外）
  - 配線: `ResultsProcessor` / `ShutubaTableProcessor` が `jockey_name` を emit、
    `_scrape_shutuba` が騎手名テキストを取得（`騎手` 列）。DataMerger の per-date ループで
    `_add_jockey_change` 実行 → ライブ側も `_merge_horse_results` 継承で自動パリティ
- FE チェーン配線: run_pipeline の builder 2 箇所 + integration テスト fixture に
  `.add_weight_change_rate().add_rotation_category()` を追加

**検証結果**:
- unit: `test_feature_engineering_extensions.py`（体重率・ローテ区分・境界・初出走フラグ）+
  `test_data_merger_features.py`（乗り替わり・テン乗り・マーク正規化・履歴なし・jockey_name drop）
  → 48 passed
- 合成 E2E: merge→FE で `jockey_name` が featured に残らない（object dtype 列 0）ことと、
  weight_change_rate/rotation/jockey_change/first_ride の生成を確認（総 386 列）
- 全 pytest 891 passed / 18 skip、mypy・ruff(src)・import-linter 4 契約 KEPT
- **リーク**: jockey_change/first_ride は過去走（date < 当日）の騎手のみ参照。
  weight/rotation は当該レースの馬体重・間隔（結果ではなく事前確定情報）

**notebook 注記**: `main.ipynb` の FE チェーン（cell 31/98/102/106）は既に production から
乖離（`add_interaction_features`/`add_race_level_zscore` を欠く旧探索版）しているため、
Phase 2 の FE メソッド追加は見送り。merger 由来の jockey_change/first_ride は notebook でも
自動生成され（後方互換）、破綻はしない。production の正は run_pipeline + integration テスト。

### （旧 Phase 2 記載）

- `FeatureEngineering.add_weight_change_rate()`: `体重変化 / 体重`。
  `RACE_LEVEL_ZSCORE_COLS_G1` に追加。学習・ライブ両チェーンに配線
- `FeatureEngineering.add_rotation_category()`: `pd.cut(interval, ROTATION_BINS)` →
  既存 `_dummify` でカテゴリ固定ダミー化。
  `ROTATION_BINS = [0, 7, 14, 28, 56, inf]`、初出走（interval NaN）は
  `rotation_first_run` フラグ
- 乗り替わり: `ResultsProcessor` に騎手名を **`jockey_name`** 列として復活
  （`騎手` 名にすると既存のサイレント無効の group_cols 集計が突然発動して列数爆発
  するため回避）。出馬表スクレイパにも騎手名テキスト抽出を追加。
  `DataMerger._add_jockey_change()`: 前走騎手との比較で `jockey_change`、
  過去騎手集合に不在なら `first_ride`（テン乗り）。
  見習マーク（☆▲△等）を除去してから比較

### Phase 3: スピード指数（項目 2）— ✅ 完了（2026-07-24）

**実装サマリ**:
- 新モジュール `src/preprocessing/_speed_index.py`（純関数・DI・I/O は save/load のみ）:
  - `build_base_time_table(horse_results, cutoff_date)`: `(開催,race_type,course_len,馬場)`
    の細キー + `(race_type,course_len)` の粗キーで time_seconds の mean/std/count を集計。
    cutoff_date 指定時は `date < cutoff` のみ使用（リーク遮断）
  - `attach_speed_index()`: `50 + 10×(base_mean − time)/base_std`（速い＝高い）。細キーの
    count < 30（`BASE_TIME_MIN_COUNT`）は粗キーへフォールバック、いずれも無ければ NaN
  - `save/load_base_time_table()`: fine/coarse を単一 CSV（`_scope` 列）で往復
- 定数 `src/constants/_speed_index.py`、artifact パス `LocalPaths.BASE_TIME_TABLE_PATH`
  （`data/master/base_time_table.csv`、`data/**/*.csv` で gitignore 済み）
- `AGG_TARGET_COLS` に `speed_index` を追加 → 多窓集計・Z-score が自動発動
- **リーク遮断**: `DataMerger._speed_index_cutoff()` が results のレース日を DataSplitter と
  同じ規則（unique race を date 昇順、`(1-test_size)` 位置）で境界日を算出。
  `_ensure_speed_index()` が学習側は cutoff 付きで build して artifact 保存、
  ライブ側（`ShutubaDataMerger`, `_speed_index_build=False`）は artifact をロード。
  `_merge_horse_results` 冒頭で horse_results に speed_index を付与
- `_summarize`/`_summarize_with` を「存在する列のみ集計」に堅牢化（speed_index 未付与でも安全）

**検証結果**:
- unit: `test_speed_index.py`（cutoff で未来レース除外・速い馬ほど高指数・粗キー
  フォールバック・min_count 未満フォールバック・no-base→NaN・save/load 往復）+
  `test_data_merger_features.py::TestSpeedIndexCutoff`（境界日の算出）→ 44 passed
- 合成 E2E: 学習 build → artifact 保存 → ライブ load の両経路で `speed_index_mean_5R` 等が
  非 NaN 生成されることを確認
- 全 pytest 903 passed / 18 skip、data/master への意図しない書込み無し、
  mypy・ruff(src)・import-linter 4 契約 KEPT
- **cutoff の構造テスト**が要（ラベルシャッフルは分布リークを検出できないため）→ 実装済み

**留意**: retrain の `test_size` は既定 0.2 で `SPEED_INDEX_TEST_SIZE` と一致。test_size を
変更した場合は基準タイム表の再生成（次回 retrain）で cutoff が追従する。全期間算出との
AUC 差の 1 回比較は実データ環境で実施予定（差が無ければ現行の train 限定を維持）。

- 新モジュール `src/preprocessing/_speed_index.py`（純関数・DI・Streamlit 非依存）:
  - `build_base_time_table(horse_results, cutoff_date)` — `(開催, race_type,
    course_len, 馬場)` 別の time_seconds mean/std/count。count < 30 のセルは
    `(race_type, course_len)` の粗いキーへフォールバック
  - `attach_speed_index()` — `speed_index = 50 + 10 × (base_mean − time) / base_std`
    （IDM 風・速いほど高い）。斤量補正は第 2 イテレーションの引数オプション
- **リーク方針**: 基準タイムは train 期間限定で算出（cutoff = DataSplitter の
  test 境界日）し、`data/master/base_time_table.csv` に artifact 保存。
  DataMerger は無ければその場計算+保存、ShutubaDataMerger は必須ロード。
  全期間算出との AUC 差は Phase 3 検証で 1 回だけ比較し以後固定
- 配線: horse_results に `speed_index` 列を付与 → `AGG_TARGET_COLS` に追加するだけで
  多窓集計 + Z-score が自動発動
- unit test で「cutoff 以降のレースが base_table に寄与しない」ことを担保
  （ラベルシャッフル検査では分布リークを検出できないため構造テストが必須）

### Phase 4: レース展開予測 + 種牡馬距離適性（項目 8, 9）— ✅ 完了（2026-07-24）

**実装サマリ**:
- 展開予測 `DataMerger._add_race_pace_forecast(results)`: per-date ループの
  `_add_pace_stats` 直後（同一レース全馬の leg_type_binary/pace_median 確定後）に配置。
  `race_front_rate`（逃/先馬率, NaN 脚質は分母除外）/ `race_front_count` /
  `race_pace_mean`（想定ペース）/ `own_vs_race_pace`（自馬の相対位置）を横集計で付与。
  レース内定数のため **Z-score 対象外**（own_vs_race_pace 含め RACE_PACE_FEATURE_COLS を
  named 群に入れない）
- 種牡馬距離適性 `DataMerger._add_sire_distance_stats(results, date)` + `_dist_band()`:
  距離帯 `DIST_BAND_EDGES`（≤13 短距離/14-17 マイル/18-21 中距離/≥22 長距離、100m 単位）×
  種牡馬で `sire_win_rate_distband` / `sire_avg_rank_distband` / `sire_n_distband` を集計。
  `SIRE_DISTANCE_FEATURE_COLS` は Z-score named 群へ合流。件数の少ないセルは NaN のまま
- 双方 per-date ループ内のため ShutubaDataMerger でも `_merge_horse_results` 継承で自動パリティ

**検証結果**:
- unit: `test_data_merger_features.py`（front_rate/count・NaN 脚質除外・own_vs・
  dist_band マッピング・距離帯別勝率・temp キー drop）→ 42 passed
- 合成 E2E: merge→FE で Phase 4 全列生成、`sire_win_rate_distband_z` は Z-score される一方
  `race_front_rate_z` は生成されない（レース定数）ことを確認（総 428 列）
- 全 pytest 911 passed / 18 skip、mypy・ruff(src)・import-linter 4 契約 KEPT
- **リーク**: 展開予測は当日出走馬の過去成績のみ、距離適性は date < 当日の産駒成績のみ参照

- 展開予測: per-date ループ内 `_add_pace_stats` の**直後**に
  `_add_race_pace_forecast(results)` を追加（この時点で同一レース全馬の
  leg_type_binary が確定済み。ループ内実装なら ShutubaDataMerger に無変更で継承）:
  - `race_front_rate` / `race_front_count`（逃げ先行馬の割合・頭数）
  - `race_pace_mean`（想定ペース proxy）
  - `own_vs_race_pace`（自馬脚質のレース内相対位置）
  - レース内定数列は Z-score 対象に**加えない**（z が 0/NaN になるため）
- 種牡馬距離適性: `_add_sire_stats` を拡張。距離帯（≤1300 短距離 / 〜1700 マイル /
  〜2100 中距離 / ≥2200 長距離）× sire の勝率・平均着順・件数。
  count 小のセルは NaN のまま（LightGBM に委ねる）

### Phase 5: 馬主・生産者成績 + ライブ側 entity-stats artifact（項目 4）— ✅ 完了（2026-07-24）

**実装サマリ**:
- 新モジュール `src/preprocessing/_entity_stats.py`（純関数）: `compute_entity_stats`
  （id 別の直近 N レース勝率/相対平均着順、空でも列付き空表）+ `save/load_entity_stats` +
  `entity_stats_path`（`data/master/entity_stats_<id>.csv`）
- `DataMerger._add_owner_breeder_stats`（`OWNER_RECENT_N=100`）: 学習側は self._results の
  過去行から owner/breeder 勝率を集計。breeder_id は `merge()` 冒頭の `_attach_breeder_id()`
  で results へ事前 join（`_merge_horse_info` の重複列除去で二重化を防止）
- `OWNER_BREEDER_FEATURE_COLS` を Z-score named 群へ合流
- **既存バグの是正（重要）**: `jockey_win_rate` 等は学習では過去行から算出されるが、ライブ推論
  （ShutubaDataMerger の self._results は出馬表のみ＝履歴空）では全 NaN に化け reindex fill 0
  頼みだった。学習側 `merge()` 末尾で `_save_entity_stats_snapshot()` が最新スナップショットを
  4 エンティティ分保存し、ライブ側は `_merge_loaded_entity_stats` でこれをロードしてマージ
  （owner_id/breeder_id は horse_info から補完）。学習時点の統計＝推論時点で利用可能な過去情報
  なので point-in-time 正当・リークなし。jockey/trainer/owner/breeder 4 種すべてに適用
- artifact 欠如時も列を NaN で必ず生成し学習/ライブの列パリティを維持

**検証結果**:
- unit: `test_entity_stats.py`（勝率/相対着順/直近 N/空表/往復）+
  `test_data_merger_features.py::TestOwnerBreederStats`（学習集計・**ライブ artifact ロードで
  非 NaN**・artifact 欠如で NaN 列・jockey ライブ分岐）→ 54 passed
- 合成 E2E: 学習 build → 4 artifact 保存 → ライブ load で `jockey_win_rate` が [0.5,0.5]
  非 NaN（是正確認）。owner/breeder も同様。owner/breeder は Z-score される
- 全 pytest 923 passed / 18 skip、data/master への意図しない書込み無し、
  mypy・ruff(src)・import-linter 4 契約 KEPT

これにより「§1 既知のサイレントバグ 2」のライブ集計特徴量パリティ問題を解消。

- `DataMerger._add_owner_breeder_stats()` を `_add_jockey_trainer_stats` と同型で新設。
  breeder_id は `merge()` 冒頭で horse_info から results へ 1 回だけ事前 join。
  `OWNER_RECENT_N`（例 100）を定数化
- **ライブパリティの是正（既存バグ対策を兼ねる）**: 学習時に最新時点の entity 統計を
  `data/master/entity_stats_{jockey,trainer,owner,breeder}.csv` に保存
  （新設 `src/preprocessing/_entity_stats.py`）。ShutubaDataMerger は自前の過去行が
  空のとき artifact をロードしてマージ（学習時点の統計 = 推論時点で利用可能な
  過去情報なので point-in-time 的に正当・リークなし）
- test_live_prediction で「ライブ側の jockey_win_rate / owner_win_rate の非 NaN 率 > 0」
  を assert（既存バグの回帰テストを兼ねる）

### Phase 6: グループB（追加スクレイピング）— 6-a 完了 / 6-b はゲート保留（2026-07-24）

**6-a 事前調査（実装済み）**:
- `scripts/probe_netkeiba_free.py`: フェッチ(I/O)と解析(純関数)を分離。
  `analyze_training_page` / `analyze_shutuba_free_extras` / `training_verdict` は
  ログイン壁・プレミアムマーカー・調教タイム/ラップ/評価の有無を検出し、
  PROCEED / SKIP_TRAINING / INCONCLUSIVE を判定。CLI は PlaywrightScraper +
  `_rate_limiter` 経由で取得し HTML を保存（netkeiba アクセス可能環境で手動実行）
- unit: `tests/preparing/test_probe_netkeiba_free.py`（無料ページ/会員壁/文脈なし/
  出馬表無料枠/verdict 分岐）→ 10 passed。ruff clean・全 pytest 933 passed

**6-b 実装（ゲート保留）**:
- **判定ゲート未通過のため未実装**。ユーザーは netkeiba 非会員で、調教（追い切り）データは
  会員限定の可能性が高い。実サイトでの `probe_netkeiba_free.py` 実行結果が **PROCEED** の
  場合のみ `_scrape_training.py` / `raw_training` テーブル / `_training_processor.py` を実装する
- **SKIP_TRAINING の場合の代替**: item 8（レース展開予測）は Phase 4 で内製済みのため、
  調教データが取れなくても展開・脚質シグナルは確保されている
- 実行手順（ユーザー、VPS 等で）:
  `python scripts/probe_netkeiba_free.py --race-id <12桁> --horse-id <id>` →
  出力の verdict と保存 HTML を確認 → PROCEED なら 6-b に進む旨を共有

### Phase 7: 残ギャップの取り込み（追加スクレイピング不要）— ✅ 完了（2026-07-24）

主要ファクター監査（Phase 0〜6 後）で見つかった低コストな穴のうち、既存 raw データで
実装できるものを取り込む。

**実装サマリ**:
- **7-1 着差・賞金の過去走多窓集計**: `AGG_TARGET_COLS` に `着差`/`賞金` を追加
  （能力差・レース格の履歴）。`HorseResultsProcessor` で `着差` の to_numeric、
  `賞金` はカンマ除去 + to_numeric
- **7-2 母父(BMS/damsire, peds_2)産駒成績**: `_add_sire_stats` を汎用 `_add_pedline_stats`
  にリファクタし、`_add_damsire_stats`（peds_2 で damsire_win_rate/avg_rank/recent）を追加。
  `_separate_by_date` の peds 事前 join を peds_0+peds_2 に拡張。`DAMSIRE_FEATURE_COLS` を
  Z-score 群へ
- **7-3 競馬場別の馬成績**: `_add_course_condition_stats` を拡張し `win_rate_at_place` /
  `avg_rank_at_place`（同一競馬場での勝率/相対着順）。results 開催(place_id Int64) と
  horse_results 開催(PLACE コード str) を 2 桁ゼロ埋め文字列に正規化して照合。
  `PLACE_CONDITION_FEATURE_COLS` を Z-score 群へ
- 全て per-date ループ内 or 集計機構経由のため ShutubaDataMerger でライブ自動パリティ

**検証結果**:
- unit: `test_data_merger_features.py`（競馬場別勝率/母父勝率/着差・賞金集計/place 列なし
  スキップ/temp キー drop）→ 50 passed。既存 sire/course-condition 回帰なし（refactor 同値）
- 合成 E2E で damsire/place の Z-score 生成確認
- 全 pytest 937 passed / 18 skip、mypy・ruff(src)・import-linter 4 契約 KEPT
- **リーク**: すべて過去走（date < 当日）のみ参照

**Phase 7 で見送った項目（理由）**:
- **種牡馬×馬場適性**: horse_results の `馬場` 語彙（良/稍/重/不）と results の
  `ground_state1/2`（芝/ダ別）の突合が実データでの語彙確認を要するため保留
- **競走馬種別（内外国産）**: 産地(ORIGIN) の raw 収集有無が実データで未確認（HorseInfoProcessor
  で未選別。取込には収集確認 → 選別 → 内外国産導出が必要）
- **レース名（タイトル）**: race_class と冗長のため不採用
- **叩き2戦目**: 休養明けからの連続戦数カウント（逐次ロジック）で中コスト、今回見送り
- **ゲートの速さ・配合ニックス**: データ源/設計が別途必要

### Phase 8: 交互作用の拡充 + 前走単独 — ✅ 完了（2026-07-24）

ファクター再監査（相互作用の重要性の指摘を受けて）で見つかった低コストな穴を取り込む。

**実装サマリ**:
- **8-1 交互作用 3 種**（`_interaction_features.py`）: `age_x_distance`（年齢×距離＝若駒の
  距離替わり）/ `age_x_weight`（年齢×馬体重＝若齢戦の馬格）/ `frame_x_field`（枠番×頭数＝
  少/多頭数で枠の価値が変化）。`INTERACTION_FEATURE_COLS` に追記
- **8-2 前走単独**（`DataMerger._add_prev_race_stats`）: 窓集計と別に「直前走そのもの」の生値を
  `prev_rank`/`prev_rank_diff`/`prev_final_corner`（4角位置）/`prev_nobori`/`prev_speed_index`
  として付与。`PREV_RACE_FEATURE_COLS` を Z-score 群へ（直近フォームのレース内相対比較）
- 双方 per-date ループ内 or FE チェーン内でライブ自動パリティ・過去走のみ参照でリークなし

**検証結果**:
- unit: `test_interaction_features.py`（3 交互作用の値・欠損スキップ）+
  `test_data_merger_features.py::TestAddPrevRaceStats`（最新走判定・全列・空スキップ）→ 67 passed
- 合成 E2E で prev_rank の Z-score 生成確認
- 全 pytest 944 passed / 18 skip、mypy・ruff(src)・import-linter 4 契約 KEPT

**Phase 8 で見送った項目（理由）**:
- **クラス替わり**: 過去走(horse_results)にクラス列が無く、レース名からのクラス抽出 +
  順序エンコードが必要（中コスト）
- **遠征・所属（東西 美浦/栗東）**: 調教師の所属マスタが未収集（trainer_id のみ保持）
- **騎手×コース / 父×馬場**: 条件付き集計の拡張（中コスト）。父×馬場は Phase 7 と同じ
  馬場語彙の実データ確認が前提
- 直線長・坂・コーナー形状: コース定数テーブルが別途必要（**競馬場別成績で実質吸収**済み）

### Phase 6: グループB（追加スクレイピング）— 事前調査ゲート付き（原計画）

- **6-a 事前調査（実装より先）**: `scripts/probe_netkeiba_free.py` で非ログイン状態の
  以下を取得し、ログイン壁/プレミアムマーカーの有無・調教タイムセルの有無を判定:
  - `race.netkeiba.com/race/oikiri.html?race_id=...`（レース別追い切り）
  - `db.netkeiba.com/horse/training/{horse_id}`（馬別調教）
  - 出馬表ページの無料枠（脚質マーク・展開図・馬体重発表欄）の DOM 確認
- **判定ゲート**: 無料で調教タイム（評価ランクのみでも可）が取れる場合のみ 6-b へ。
  取れなければ項目 11 はスキップと記録（項目 8 の内製展開予測が代替）
- **6-b 実装（ゲート通過時のみ）**: `src/preparing/_scrape_training.py`
  （bin 保存 → parse の既存 2 層流儀）、`TABLE_SPECS` に `raw_training`
  （PK: race_id, 馬番）、`_training_processor.py`。
  歴史データのバックフィルが揃うまで学習列に入れない（全 NaN 列防止）。
  ライブ側は `feature_names_` reindex fill 0 で安全

---

## 4. 検証方針（全フェーズ共通）

1. **unit**: 各 `_add_*` に合成 DataFrame で手計算一致テスト。特に「date < 当日」境界
   （当日レースが集計に混入しないこと）を全メソッドで明示テスト
2. **パリティ**: test_live_prediction に「新特徴量列がライブ featured に存在し
   非 NaN 率が閾値以上」の assert をフェーズごとに追記
3. **リーク**: フェーズ末に `python scripts/leakage_check.py`（シャッフル AUC ≈ 0.5 で PASS）
4. **効果測定**: フェーズごとに retrain → auc_test をベースラインと比較 +
   feature importance 記録。効果ゼロ〜マイナスのファクターは定数リストから外すだけで
   ロールバックできる構造を維持

## 5. フェーズ依存関係

```
Phase 0 → Phase 1 → Phase 3（speed_index は AGG_TARGET_COLS 機構に乗る）
Phase 0 → Phase 2 → Phase 4a（展開予測は pace 系整備済みが前提）
Phase 0 → Phase 4b（種牡馬距離適性、独立）
Phase 0 → Phase 5（artifact パターンは Phase 3 と流儀を揃える）
Phase 6a（調査）は並行可 / 6b は 6a ゲート通過後
```

## 6. 主要変更ファイル

- `src/constants/_feature_cols.py` — AGG_TARGET_COLS/AGG_GROUP_COLS 一元化 + 全新定数
- `src/preprocessing/_data_merger.py` — per-date ループへの `_add_*` 追加、
  speed_index 付与、展開予測、種牡馬距離適性、馬主/生産者集計
- `src/preprocessing/_horse_results_processor.py` — corner_rate 追加・数値化
- `src/preprocessing/_feature_engineering.py` — 体重増減率・ローテーション、Z-score 合流
- `src/preprocessing/_speed_index.py`（新設）/ `_entity_stats.py`（新設）
- `src/preprocessing/_shutuba_data_merger.py` — artifact ロード分岐（ライブパリティ）
- `src/preprocessing/_results_processor.py` — jockey_name 復活
- `src/preparing/_scrape_shutuba.py` — 騎手名テキスト・無料枠拡張
- `src/pipeline/run_pipeline.py` — builder の定数参照化、artifact 保存フック
- `scripts/probe_netkeiba_free.py`（新設・Phase 6a）

## 7. 対象外としたもの（理由）

- **風向・風速・気温**（グループC）: netkeiba に項目がなく外部気象データ源+
  競馬場マッピングが必要。今回スコープ外（ユーザー判断）
- **入厩日・厩舎コメント・タイム指数**: netkeiba 会員限定（非会員のため対象外）
- **パドック観察・騎手の目つき等**: 定量データ源が存在しない
- **当該レースのオッズ・人気の特徴量化**: リーク源として既存設計どおり除外を維持
  （オッズは別系統のオッズ力学モデルが担当）
