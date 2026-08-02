# JRDB 特徴の lineage と実体化修復（続31 監査の後始末）

## 監査結論（2026-08-02）

- **raw ingestion / parser boundary: 合格**（15形式 offset 境界 OK・store 主要テーブル populate）。
- **current featured の JRDB 網羅性: 不合格（定義43特徴中 5 のみ実体化・38 ABSENT）**。
- 原因は**データ欠損でなく、feature attach/build 経路の未適用または古い artifact**。
- 既存実験（B/P2/H3）は**実際に使用した特徴については有効**。ただし「JRDB 探索完了・残るは映像のみ」という
  広い結論は**撤回・限定**（探索空間が実装上閉じていなかった）。正しくは「**現 featured に実体化されていた
  特徴集合については探索済み。定義済み38特徴＋取得済み複数ソースは未実体化・未ブリッジ**」。

## 根本原因（2経路乖離）

1. **本線**: `_adapter.build_raw_results`（`_KYI_INDEX_COLS`）→ `_results_processor`（列選択:119）が
   **固定5列のみ**注入: `jrdb_idm, jrdb_kijun_odds, jrdb_kyakushitsu, jrdb_joho_idx, jrdb_kishu_idx`。
2. **完全 augment**: `build_kyi`(KYI_FEATURE_MAP 全)＋`build_history`(prev_*)＋`build_soten_history`(MySpeed)
   ＋`attach`(kijun_gap) は `scripts/jrdb_build_features.py`（standalone・別 pickle `data/featured_jrdb.pkl`）
   **にのみ存在し本線 featured に未配線**。→ 研究 featured には 5 しか無い。

現状の source 状態（`scripts/audit_acquired_items.py` L4）:
IMPLEMENTED_NOT_APPLIED（KYI 残指数 / SED prev_*・MySpeed / SKB prev_trouble）、
INGESTED_NOT_BRIDGED（TYB/CYB/CHA/KKA/UKC/SRB/KAB/BAC）、
HISTORICAL_ONLY（KSA/CSA 2026 のみ）、INGESTION_MISSING（KTA=0）、OUTCOME_ONLY（HJC）。

## lineage（38 実体化のための追跡表・repair の骨子）

各特徴について次を一本化する: parser field → store column → augment 関数 → build 呼出し → join key →
output column → 最終列選択。まず attach 実装のある KYI/SED/SKB の 38 列が最優先。

| 群 | parser/source | augment 関数 | join key | output | 現状 | 時点クラス |
|---|---|---|---|---|---|---|
| KYI 指数群(33) | KYI @55.. | `build_kyi`/KYI_FEATURE_MAP | (race_id,馬番) | jrdb_* | 5のみ本線・28未適用 | direct_current |
| pace 予想 | KYI pace_yosou | `build_kyi`(_HMS) | (race_id,馬番) | jrdb_pace_hms | 未適用 | direct_current |
| 基準乖離 | KYI kijun_odds÷単勝 | `attach` | (race_id,馬番) | jrdb_kijun_gap | 未適用 | direct_current |
| 前走トラブル | SED deokure / SKB tokki | `build_history` | ketto×ymd(asof) | prev_deokure/prev_trouble | 未適用 | historical_only |
| MySpeed | SED soten | `build_soten_history` | ketto×ymd(asof) | jrdb_ms_* | 未適用 | historical_only |

## 修復手順（性能を見る前）

1. **feature build を fail-closed 化**（実装済: `src/training/_feature_materialization.py`）。
   `assert_features_materialized(featured.columns, REQUIRED_JRDB_MIN, optional=EXPECTED_JRDB_FULL)`。
   required=現行5（退行検知）、optional=全43（欠落を warn 可視化）。build/train の featured ロード直後に呼ぶ。
2. **attach 経路を本線へ配線**（`scripts/jrdb_build_features.py` の augment を featured 生成に統合。または
   `_results_processor` の固定5列を KYI_FEATURE_MAP 全 + pace_hms + kijun_gap に拡張し、prev_*/MySpeed は
   ketto×date asof で結合）。同名 `ten_idx`/`agari_idx` が KYI(予想) と SED(実測)に両在するため、**列名でなく
   source×timestamp**で時点を判定し feature contract に `direct_current/historical_only` を持たせる。
3. **再構築後は feature-only 再監査**（性能でなく: 列存在・年別 coverage・sentinel 率・unique・race内分散率・
   source timing class・join 一致率・NaN/inf・target より未来の参照数）。ここで feature artifact の hash を保存。
4. **新研究は standing protocol**（`docs/temporal_split_protocol.md`）: 2015-2024 development_known で
   選択/freeze、2025-2026 は burned_for_evidence（refit 可・選択不可）、未観測 tranche で一度だけ clean test。
   実体化された38特徴は**未観測ゆえ新仮説として clean に検証可能**（selection を 2015-2024 に限る）。

## 妥当性リスク（実体化と同時に対処）

- **sentinel の中央未クレンジング**: idx/3F の `-99.9`/負値/JRDB fill。materialize 時に統一クレンジング
  （`clip_3f` 相当を全 jrdb_ 数値へ）。
- **KSA/CSA は 2026 のみ**（HISTORICAL_ONLY）: 時系列特徴には過去年 KZA/CZA が要る。現行 master 用途と分離。
- **KTA=0**（INGESTION_MISSING）: file/glob/parser/store のどこで落ちたか要調査。
- **行数差の anti-join 分類**: SKB−SED=−1,014 / KKA−SED=−995 / UKC−ketto=−4 / BAC−HJC=−35 / TYB−SED=+18 を
  取消・除外・master 欠損・parser drop・file 欠落へ分類して store 層を認定完了にする。

## 優先順位

新規ソース追加より **既に定義・実装済みの38特徴を本線 featured へ正しく実体化**が最優先。以降:
KYI 未実体化列 → SED/SKB strictly-prior → CYB/CHA 発走前調教 → TYB 時刻保証付き直前 →
KKA/UKC/BAC/KAB → SRB 過去走集約 → KTA 取込修復・KSA/CSA 過去年 coverage。
