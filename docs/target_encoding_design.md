# 時系列 OOF Target Encoding 実験設計（TE_ENCODE・独立仮説）

**状態: 条件付き承認・凍結保留（sire/damsire 監査の完了待ち）**。目的関数比較とは**独立した新規仮説**。
**凍結条件3点**（全て満たしたら凍結→実装）:
1. **日次バッチ単位の strictly-prior 計算**へ修正（行単位 `shift(1)` は同一レース内 行順リークの恐れ）。← 反映済
2. **primary target を `rank_win`（1着）へ固定**。← 反映済
3. **sire/damsire の実在性・品質監査を完了**（`scripts/audit_pedigree_keys.py`）。← **未（ユーザ実行待ち）**

## 問い（1つ）

> 騎手・調教師・種牡馬等のカテゴリを、**時系列 OOF・平滑化・strictly-prior** に target encoding した特徴を、
> 現行 production **BINARY** モデルに追加すると、レース内 softmax 勝率の **LogLoss が development で
> 一貫改善するか**（市場 `log q` を入れた上での純増分）。

**プロトコル位置づけ**: 特徴価値の検証＝**selection（development 2015-2024）で実施可・reserved tranche
非消費**。有望なら features/平滑化/OOF 手順を凍結し **2027 で一度だけ**確認（Holm 更新）。ROI は non-evidential。
JRDB42 の NLL 確認や既確定結論には触れない。ベースラインは**目的関数比較で追認した BINARY**。

## エンコード対象カテゴリ（階層・backoff・列数事前固定）

**Level 1（主効果）**: 騎手 jockey_id / 調教師 trainer_id / 種牡馬 sire / 母父 damsire。
**Level 2（交互作用）**: 騎手×競馬場 / 種牡馬×距離帯 / 種牡馬×芝ダ / 調教師×クラス。

- 交互作用 TE は全体平均でなく**対応する主効果へ backoff**:
  `TE_{a×b} = (n_{ab}·ȳ_{ab} + α·TE_a) / (n_{ab} + α)`。未知の交互作用は主効果 TE_a、主効果も未知なら全体 prior μ。
- **距離帯・クラス区分は実行前に固定**し、結果を見て境界を変えない（区分定義を本書に列挙して凍結）。
- カテゴリ×交互作用の**列数は事前固定**（探索的な列選別をしない）。

**primary = 全 TE をまとめた `+TE_FULL`**。secondary ablation は**事前固定の4群**のみ:
(1) entity main effects、(2) jockey interaction、(3) pedigree interaction、(4) trainer interaction。
個々の列を探索的に選別しない。

## ターゲットと平滑化

- **primary target = `rank_win`（1着）に固定**。評価対象が勝者 LogLoss の BINARY ゆえ TE も同一 estimand に揃える。
  `rank`(top3) TE は **secondary exploratory**（別 estimand＝「target を変える」追加仮説）で、**2027 候補の採否には
  使わない**。複勝モデルを評価する別スレッドでは逆に top3 を primary にする。
- 平滑化（経験ベイズ縮約）: `TE_g = (n_g·ȳ_g + α·prior) / (n_g + α)`、prior=主効果は全体 causal 平均 μ、
  交互作用は対応主効果 TE（backoff）。未知カテゴリは prior。

## 時系列 OOF 構築（リーク防止＝本実験の核・**日次バッチ strictly-prior**）

**同日リーク規約: 全レースで「前日まで」に丸める**。race r の TE は `date < date_r` の結果のみ使用（同日前半
レースの結果も使わない）。時刻不明日だけ規則を変えない。保守的だが、発走順/確定時刻の不整合・失格や着順変更・
実運用の再計算タイミング・**同一レース内の行順リーク**を一括排除できる。当日逐次更新 TE は将来の独立仮説。

**実装は馬行単位の `shift(1)` でなく日次バッチ集約**（行順リークを構造的に排除）:

    1) (category, date) ごとに count / target_sum を集計（日次バッチ）
    2) category 内で **日付順** に cumulative sum（当日集計は含めない＝strict past）
    3) その "前日までの n_g, Σy_g" を元の馬行へ (category, date) で join
    4) TE = (Σy_prior + α·prior) / (n_prior + α)  ［prior は主効果 μ / 交互作用は backoff TE_a］

これで同一 race・同一日の結果は確実に入らない（行順に依存しない）。

- **walk-forward OOF**: fold ごとに TE を再計算（train fold の過去分だけで prior を作り valid fold に適用）。
  fold 内でも上記「前日まで」を守る（train を丸ごと使う naive OOF は同一 fold 内リークゆえ不可）。
- モデルは追認済 **BINARY 固定**に causal TE 列を追加（CatBoost ordered TS は使わない）。

**leak 全件監査（必須）**: TE 値が参照した最大 source 日付 **< 当該レース日**・**同一 race 参照=0・同日参照=0・
未来参照=0** を全 target 行で集計・fail-closed（`_leak_audit` と同型の manifest）。

## arm と評価

| arm | 特徴 |
|---|---|
| **BASELINE** | production BINARY の現行特徴（＋レース内 z＋`log q`・目的関数比較と同一土俵） |
| **+TE** | BASELINE ＋ 上記 causal TE 列 |

- 同一 development walk-forward OOF（`rolling_folds`）・同一 fold・BINARY 目的で学習。
- 確率＝レース内再正規化（softmax(log p)）。較正は OOF（必要なら temperature/isotonic を両 arm 同手順）。
- 指標: **LogLoss(nats/race)**・**ECE**・NDCG@1,@3（参考）・edge 分位別 ROI（非証拠）・年別再現性。

## α（縮約強度）の決め方（事前固定）

- **第一推奨**: outer fold の **training 期間だけ**を使った empirical Bayes 推定（カテゴリ頻度に応じた prior
  strength・恣意的グリッド不要）。outer validation には一切触れない。
- **実装負荷を抑える場合**（固定 inner-CV 方式・以下を事前固定し変更しない）:
  - grid = `{5, 20, 50, 100, 200}`、全 TE 列で**単一の共通 α**、inner も rolling-origin、
  - 選択指標 = inner OOF LogLoss、同率は**大きい α**、outer には触れない、
  - α 選定は secondary multiplicity に**数えない**代わりに grid を変更しない。
- カテゴリごと・fold ごとに自由な grid を探索しない（機械性優先ならこの固定 inner-CV で十分）。

## 仮説構造・採否基準（事前固定）

- **PRIMARY（m=1）**: `+TE_FULL − BASELINE` の ΔLogLoss。
- **SECONDARY（Holm m=4）**: 事前固定4群 (entity main / jockey interaction / pedigree interaction /
  trainer interaction) の ΔLogLoss。**PRIMARY が通過した場合のみ解釈**する。
- **採用条件**: ΔLogLoss の venue×日 block bootstrap paired **CI 上限 < 0** かつ **|Δ| ≥ MES=0.001** かつ
  **ΔECE ≤ +0.005** かつ **改善 fold 数 ≥ 4/7**（年別過半を明示）。ROI は判定に使わない。
- 満たせば「dev で有望」＝2027 事前登録候補（**`+TE_FULL` をそのまま凍結**）。**ablation 結果を見て FULL から
  都合よく列を削ったモデルを 2027 候補にしない**（cherry-pick 禁止）。満たさねば**登録せずクローズ**。

## 妥当性チェック（実装時）

- TE の **leak manifest**（未来/同一race 参照=0）を fail-closed。
- カテゴリ coverage（未知率）・α 選定の記録（development のみ）・列数固定。
- 市場との交絡: `log q` を入れた上での純増分を見る（TE が単に人気の再現でないか＝±`log q` ablation を secondary）。
- BASELINE と +TE は**同一 fold・同一 seed・同一ハイパラ**（TE 以外の自由度を固定）。

## 再利用資産（実装時・再発明しない）

- `filter_selection_domain` / `rolling_folds`（development walk-forward・selection 域 fail-closed）。
- `block_bootstrap_ci`（venue×日 paired CI）・`race_softmax_probs`/`race_nll`（listwise 純関数・既 tests）。
- BINARY 学習は `run_objective_comparison._fit_scores` の BINARY 経路 or `_keiba_ai`/`_model_wrapper`。
- `_leak_audit.strictly_prior_join_report` と同型の TE-leak manifest（新規・純関数＋tests）。
- **新規の本質部分**: causal 時系列 OOF TE builder（`groupby(cat).cumsum().shift(1)` ベース・純関数・要 tests）。

## 確定回答（ユーザ・2026-08-02）

1. カテゴリ = Level1(4主効果)＋Level2(4交互作用・backoff)・列数事前固定・primary=+TE_FULL・secondary 4群。✅
2. primary target = **`rank_win`**。top3 は secondary exploratory（2027 採否に使わない）。✅
3. 同日リーク = **全レース前日まで**（`date < date_r`・日次バッチ集約）。✅
4. α = outer-train empirical Bayes（第一推奨）／固定 inner-CV grid{5,20,50,100,200}・共通α・変更しない。✅
5. **sire/damsire materialize = 凍結前に監査必須**（`scripts/audit_pedigree_keys.py`）。存在確認前に「4主効果」を
   primary 固定できない。母父欠損が多くても**都合よく外さず**、凍結前に対象集合を改訂する。**←未完了**

## sire/damsire 品質監査（凍結条件3・要ローカル）

`scripts/audit_pedigree_keys.py --featured data/featured_jrdb.pkl` で最低限:
列名・非欠損率・unique 数・年別非欠損率・**ID の時系列安定性（同一馬で父/母父 ID が不変か）**・
**文字列名と数値 ID のどちらが正規キーか**を出す。配線が無ければ UKC materialization を**別作業で先に実施**し、
値を見ずに変換規則・品質基準を固定してから対象集合を確定する。

## 実装（凍結後・着手可の checklist）

- [ ] causal 日次バッチ TE builder（`(cat,date)` 集約→category 内日付 cumsum→当日除外→馬行 join・backoff）を
      **純関数**で実装し tests（行順非依存・同日/未来非参照・backoff・未知カテゴリ→prior）。
- [ ] TE-leak manifest（`_leak_audit` 同型・同一race/同日/未来 参照=0 を fail-closed）。
- [ ] `scripts/run_target_encoding.py`：BASELINE vs +TE_FULL＋4群 ablation を同一 fold/seed/ハイパラ・
      `rank_win`・development walk-forward（`rolling_folds`/`filter_selection_domain`）で学習。
- [ ] α は outer-train empirical Bayes（or 固定 inner-CV）・outer に触れない。
- [ ] 指標 LogLoss/ECE/NDCG(参考)/edge分位ROI(非証拠)/年別符号、ΔLogLoss venue×日 block bootstrap paired CI＋
      PRIMARY(m=1)/SECONDARY(Holm m=4・PRIMARY 通過時のみ解釈)。
- [ ] 実行は development のみ（2025+ fail-closed）・採否は dev で確定しない（有望なら +TE_FULL を凍結し 2027 一度だけ）。

## 再利用資産

- `filter_selection_domain`/`rolling_folds`・`block_bootstrap_ci`・`race_softmax_probs`（既 tests）。
- BINARY 学習は `run_objective_comparison._fit_scores` の BINARY 経路。
- `_leak_audit.strictly_prior_join_report` と同型の TE-leak manifest（新規・純関数＋tests）。
- **新規の本質部分**: causal 日次バッチ TE builder（純関数・要 tests）。
