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

### Phase 0: 基盤整備（全フェーズの前提）

- `src/constants/_feature_cols.py` に `AGG_TARGET_COLS = ["着順"]` /
  `AGG_GROUP_COLS = ["騎手"]` を新設し、`run_pipeline.py`（builder 2 箇所）・
  `tests/integration/test_live_prediction.py`・main.ipynb に散在するリテラルを定数参照化
  （学習/ライブのパリティ事故の温床を解消）
- test_live_prediction に「学習 featured 列とライブ featured 列の差分」を明示 assert する
  パリティテストを追加
- **検証ゲート**: 既存 pytest 全通過 + retrain でベースライン AUC（0.772〜0.774）と
  feature importance を記録（以後の比較基準）

### Phase 1: target_cols 拡張 + 通算成績（項目 1, 3, 10）— コスト最小・効果大

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

### Phase 2: FE 系 + 乗り替わり（項目 5, 6, 7）

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

### Phase 3: スピード指数（項目 2）— 本命ファクター

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

### Phase 4: レース展開予測 + 種牡馬距離適性（項目 8, 9）

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

### Phase 5: 馬主・生産者成績 + ライブ側 entity-stats artifact（項目 4）

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

### Phase 6: グループB（追加スクレイピング）— 事前調査ゲート付き

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
