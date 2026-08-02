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

## 続36 修復（standalone 実データ検証後の DEAD 列 lineage 修復）

standalone augment を実データで検証し、**列実体化は成功（33 OK・0 ABSENT・既知5列 corr=1.0）**。
残る「薄い/DEAD」10 列を分類し、性能評価でなく lineage を修復した:

| 列 | 分類 | 根本原因 | 修復 |
|---|---|---|---|
| jrdb_pace_hms | MATERIALIZED_RACE_CONTEXT | 場の展開予想＝race 内定数（馬間分散≈0） | 「薄い」でなく正常。分類を訂正（DEAD 扱いしない） |
| jrdb_kakutei_bataijuu | WRONG_SOURCE | 確定馬体重は発走約15分前に確定。KYI(前日〜当日朝)では 0/空 | KYI_FEATURE_MAP から**除去**。TYB(直前・T-15)の bataijuu へ移譲(bet_time_contract) |
| prev_deokure / prev_trouble | HISTORY_ATTACH_FAILED | **ketto 列衝突**: base が既に ketto を持ち(=_ensure_ketto/本線ブリッジ)、kyi 側も ketto を持つと KYI merge が **ketto_x/ketto_y に分裂**→asof の `"ketto" in f.columns` が False→prev_* 全 NA | attach で KYI merge 後に **ketto を単一列へ coalesce**(base 優先→kyi 補完)してから asof |
| jrdb_ms_last/mean3/max5/ewm/trend/npast | HISTORY_PIPELINE_FAILED | 同上（同じ ketto 列衝突） | 同上。soten 生成関数・groupby は正常で、原因は ketto 列衝突のみ |

**真因の切り分け（実データ verify --from-store）**: date有効率 既定=robust=1.000（date は問題なし）・
ketto∩(base,hist)=61,120（重複は十分）にも関わらず prev_*/ms_* が 0.000 だったことから、原因は
「前提不足」でなく attach 内の **ketto 列分裂** と特定。修復は `src/jrdb/_augment.py`（KYI merge 後の
ketto coalesce＋kakutei 除去）。加えて `_to_race_datetime`(和暦 'YYYY年MM月DD日' も解ける明示フォーマット
検出) を asof 左キーに導入＝防御的堅牢化（ISO 既存テストも回帰維持）。

**学習側 allowlist 関門**（`src/training/_feature_materialization.py::assert_training_allowlist`）:
本線の特徴選択は denylist（`_DROP_FOR_TRAIN` 以外を全採用）なので、attach 配線後に新規実体化した
JRDB 列が**黙って既存モデルへ混入**する。関門で (1) allowlist 未実体化=RuntimeError、(2) allowlist 外の
jrdb_*/prev_* 混入=RuntimeError にし、本線統合の前に**明示決定**を強制する。B 凍結は 5 特徴のまま不変。

## 続36-d 実データ再監査で history 修復を確認＋3契約に分離

ketto coalesce 修復後の `verify --from-store`（JRA2015+ 555,128行）で **8 history 列すべてが実体化**:
prev_deokure/prev_trouble=非欠測0.891、jrdb_ms_last/mean3/max5/ewm=0.887、jrdb_ms_trend=0.693、
jrdb_ms_npast=0.887（min=1・median=6）。npast の canary も過去走数として単調増加を確認＝as-of の
行復元は健全。欠測分は「初出走＝過去走なし」で正常。

**正確な内訳（EXPECTED=42・kakutei 除外後）**: 新規生成 37 列 = ACTIVE 28（+既存5=33）／CONTEXT 1
（jrdb_pace_hms）／HISTORY 8。「38 を materialize」は旧表記で撤回。

**実体化を3契約に分離**（`src/training/_feature_materialization.py`・列が在るだけの全欠測 PASS を防止）:

| 契約 | 列 | 判定基準 |
|---|---|---|
| CURRENT_ACTIVE_REQUIRED | 33（既存5＋新規28・KYI current-race） | presence + coverage + **race内分散** |
| CONTEXT_REQUIRED | 1（jrdb_pace_hms） | presence + coverage（**race分散は不問**＝全馬共通の場コンテキスト） |
| HISTORY_REQUIRED | 8（prev_*＋jrdb_ms_*） | **semantic coverage**（過去走ゼロの NaN は正常・全欠測は fail-closed） |

verify は3契約を個別に PASS/FAIL 表示し、`jrdb_pace_hms` は CONTEXT として「薄い」から除外。ketto 有効率も
all-rows と JRA2015+ eligible を分けて表示（0.830 は地方/2014以前を分母に含むため＝JRA2015+ は≈1.0）。

**本線統合の判定**: current-only（ACTIVE+CONTEXT）は ✅ 明示 allowlist 付きで統合可。完全 augment
（+HISTORY）も実データで ✅（8列 semantic coverage 達成）。既存モデルへの新規列 silent 混入は
`assert_training_allowlist` で阻止・**B frozen は従来5特徴のまま不変**。

## 続36-e 認定確定（42-feature augment contract COMPLETED）＋残す証跡

**JRDB 42-feature augment contract completed: 33 current horse-level active features、
1 race-level context（jrdb_pace_hms）、8 strictly-prior history features。ABSENT/DEAD なし。**
既存5列は corr=1.000/median比=1.000（555,128行・行数不変）、sentinel 異常なし、未実体化列なし。

**認定範囲の限定（重要）**: 「完全 augment」とは**この 42 特徴契約**のこと。取得済みだが**未ブリッジ**の
別ソース（CYB/CHA/TYB/KKA/UKC/SRB/KSA・CSA/KAB）は網羅していない（INGESTED_NOT_BRIDGED のまま）。
これらは将来の別契約（各々 source×timestamp・bet_time_contract 等）として個別に配線・監査する。

**strictly-prior leak 全件監査**（`src/jrdb/_leak_audit.py`・canary に加える manifest 証跡）:
attach と同じ merge_asof(backward, allow_exact_matches=False) を正規化日付で再現し、全 target 行で
`future_reference_count / same_day_reference_count / exact_target_reference_count = 0`、
`max_source_date / target_rows / feature_rows / target_key_duplicates` を集計。verify --from-store が
history/soten 双方でこの manifest を表示し、未来/同日参照があれば `assert_strictly_prior` で fail-closed。
canary（jrdb_ms_npast が初走 NaN→1→2… 単調増加）と合わせ history のリーク安全性を正式認定。

**本線統合時の学習側規則（freeze）**:
- 既存 B: 従来の固定5特徴のみ。新規37列を silent 追加しない（`assert_training_allowlist` で強制）。
- 新規モデル: config 指定特徴だけを使用。missing configured feature は fail-closed。
- 欠測にも意味があるため（例 jrdb_nyukyu_days・history 初走 NaN）、一律ゼロ埋めせず**モデルごとの
  欠測規則を freeze して記録**する（0 埋めは情報を潰す）。

**研究上の結論**: これは**特徴生成基盤の完成**であり、新規 28 current＋8 history の**予測価値は未評価**。
既存 B/P2/H3 は不変。新規特徴の検定は別仮説として、未接触 test tranche 開封前に 特徴集合・モデル・
多重比較 family・MES・判定規則 を固定する（2027 を B と共有するなら B 含む全仮説を開封前に一括登録）。

## 続36-f 本線配線（Task#22）＝既存モデル不変・allowlist opt-in・manifest 付き

**設計判断**: 本線の学習入力は denylist（`_DROP_FOR_TRAIN` 以外を全採用）。よって 37 新規列を
**default featured へ入れると既存 stacking モデルへ silent 混入し性能が変わる**（禁止事項）。既存
denylist を保ったまま既存モデルを不変にする唯一の方法は「新規列を default featured に載せず、
消費を明示 allowlist の opt-in にする」こと。従って:

- `DataSplitter(..., feature_allowlist=None)` を追加。**None（既定）は従来 denylist＝legacy schema に
  対する既存挙動を維持**（「byte 一致」は旧/新コードの学習行列・予測 artifact の hash を実比較した
  ときのみ使う表現。現状は既存テスト green ＝ legacy schema での挙動維持）。allowlist を渡すと構築時に
  featured を `allowlist＋保護列(date/rank/着順/単勝/horse_id/CORNER/leak列)` に絞り、以降の drop で
  残るのは allowlist のみ＝新規列は silent 混入しない。指定列が無ければ fail-closed。
- **augment artifact＋allowlist なしの拒否（続36-g・必須ガード）**: `feature_allowlist=None` は denylist
  のため、完全 augment artifact を誤って渡すと新規37列が silent 混入する。`assert_no_unguarded_augment`
  （`LEGACY_JRDB_COLUMNS`=既存5 / `JRDB_AUGMENT_ONLY_COLUMNS`=残り37）で「augment 専用列が在るのに
  allowlist 未指定」を **RuntimeError で拒否**。DataSplitter 構築時に発火。純関数なので将来の
  非 DataSplitter trainer 入口にも設置可。
- 完全 augment(42列)は opt-in build（`scripts/jrdb_build_features.py`・別 pickle）で生成し、そこで
  `strictly_prior_join_report`＋`assert_strictly_prior` の全件 manifest を出力・検証（history/soten の
  未来/同日参照=0 を fail-closed 認定）。default 本線 featured の schema は不変（既存5のみ）。
- **artifact 消費契約**（`build_training_provenance`）: 設定でなく **実際に学習へ入った列** を後日監査
  できるよう `requested_feature_allowlist`（設定）と `resolved_training_features`（実結果）を分離記録し
  `resolved_training_features_hash`（順序込み）＋`input_feature_schema_id` を刻む。DataSplitter は
  `resolved_feature_columns`/`resolved_feature_hash` を公開。
- 新規/研究モデルは feature_allowlist に「既存列＋採用する JRDB 列」を明示して opt-in。**B frozen は
  従来5特徴のまま**。欠測は一律0埋めせずモデル別規則を freeze（jrdb_nyukyu_days・history 初走 NaN 等）。

これで「本線が42を通せる基盤＋既存挙動維持＋新規列の silent 混入阻止（allowlist 未指定でも拒否）＋
leak 全件証跡＋実消費列の hash 監査」が揃う。性能評価は行わない（新規列の予測価値は未接触 test tranche
開封前の事前登録の後で別途）。

## テスト網羅（続36-g ガード行列）

| ケース | 期待 | テスト |
|---|---|---|
| legacy featured＋allowlist なし | 成功（従来挙動維持） | test_legacy_featured_denylist_ok |
| augment featured＋allowlist なし | fail-closed | test_augment_featured_without_allowlist_fails_closed |
| augment featured＋明示 allowlist | 指定列だけで成功 | test_allowlist_restricts_to_configured_features |
| allowlist に無い列 | fail-closed | test_allowlist_missing_feature_fails_closed |
| B 固定5列＋augment featured | 解決入力が厳密に5列 | test_b_five_columns_resolved_exactly_five |
| resolved hash | 安定・順序込み | test_resolved_feature_hash_stable_and_order_sensitive / provenance |

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
