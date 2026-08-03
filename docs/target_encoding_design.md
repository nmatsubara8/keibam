# 時系列 OOF Target Encoding 実験設計（TE_ENCODE・独立仮説）

**状態: 設計案（未実装・未凍結）**。実装前に本書と採否基準を固定する（ユーザ査読→条件付き承認→凍結）。
目的関数比較とは**独立した新規仮説**（あの陰性結果を受けた追加探索ではない）。

## 問い（1つ）

> 騎手・調教師・種牡馬等のカテゴリを、**時系列 OOF・平滑化・strictly-prior** に target encoding した特徴を、
> 現行 production **BINARY** モデルに追加すると、レース内 softmax 勝率の **LogLoss が development で
> 一貫改善するか**（市場 `log q` を入れた上での純増分）。

**プロトコル位置づけ**: 特徴価値の検証＝**selection（development 2015-2024）で実施可・reserved tranche
非消費**。有望なら features/平滑化/OOF 手順を凍結し **2027 で一度だけ**確認（Holm 更新）。ROI は non-evidential。
JRDB42 の NLL 確認や既確定結論には触れない。ベースラインは**目的関数比較で追認した BINARY**。

## エンコード対象カテゴリ（primary・要ユーザ確定）

| カテゴリ | キー | 備考 |
|---|---|---|
| 騎手 | jockey_id | 主力 |
| 調教師 | trainer_id | 主力 |
| 種牡馬 | sire（父） | UKC 由来（要 materialize 確認） |
| 母父 | damsire（母父） | 同上 |
| 交互作用 | 騎手×場 / 種牡馬×距離帯 / 種牡馬×馬場(芝ダ) / 調教師×クラス | 少数カテゴリは強平滑化 |

- 交互作用は**少数標本になりやすい**ため α を大きく（prior へ強縮約）。カテゴリ×交互作用の**列数は事前固定**。

## ターゲットと平滑化

- **target = production と同一**（既定 `rank`=複勝圏 top3、Win ヘッド検証時は `rank_win`）。primary は production
  既定に合わせる（別 target は secondary）。
- 平滑化（経験ベイズ縮約）: `TE_g = (n_g·ȳ_g + α·μ) / (n_g + α)`、μ=時点までの global 事前平均、α=縮約強度。
  **α は development inner-CV で選定**（`assert_selection_only_on_known`・結果〔2025+〕で選ばない）。未知カテゴリは μ。

## 時系列 OOF 構築（リーク防止＝本実験の核）

各行（時刻 t のレース）の TE は **t より過去のレース結果のみ**から作る（causal expanding prior-mean）:

1. **strictly-prior**: 当該レース自身の結果を含めない・**未来を含めない**・**同日後半→前半の逆流禁止**
   （同一 ymd 内は発走時刻順、時刻不明日はその日全体を除外 or 前日までに丸める—**要ユーザ確定**）。
2. **walk-forward OOF**: fold ごとに TE を再計算（train fold の過去分だけで prior を作り valid fold に適用）。
   fold 内でも上記 causal 条件を守る（fold=train を丸ごと使う naive OOF は同一 fold 内リークになるため不可）。
3. **実装**: カテゴリごとに時系列 sort → 累積和/件数を1つ前までシフト（`groupby(cat).cumsum().shift(1)`）で
   `n_g,Σy_g` を作り `TE=(Σy+αμ)/(n+α)`。μ も同様に全体の causal 平均。CatBoost ordered TS は使わず LightGBM
   binary に causal TE 列として渡す（モデルは追認済 BINARY 固定）。

**leak 全件監査（必須）**: TE 値が参照した最大 source 日付 ≤ 当該レース前・**同一 race 参照=0・未来参照=0** を
全 target 行で集計・fail-closed（`_leak_audit` と同型の manifest）。

## arm と評価

| arm | 特徴 |
|---|---|
| **BASELINE** | production BINARY の現行特徴（＋レース内 z＋`log q`・目的関数比較と同一土俵） |
| **+TE** | BASELINE ＋ 上記 causal TE 列 |

- 同一 development walk-forward OOF（`rolling_folds`）・同一 fold・BINARY 目的で学習。
- 確率＝レース内再正規化（softmax(log p)）。較正は OOF（必要なら temperature/isotonic を両 arm 同手順）。
- 指標: **LogLoss(nats/race)**・**ECE**・NDCG@1,@3（参考）・edge 分位別 ROI（非証拠）・年別再現性。

## 仮説構造・採否基準（事前固定）

- **PRIMARY（m=1）**: `(+TE) − BASELINE` の ΔLogLoss。
- **SECONDARY（Holm）**: カテゴリ群別 ablation（騎手のみ / 種牡馬のみ / 交互作用のみ）の ΔLogLoss。m=群数。
- **採用条件**: ΔLogLoss の venue×日 block bootstrap paired **CI 上限 < 0** かつ **|Δ| ≥ MES=0.001** かつ
  **ΔECE ≤ +0.005** かつ **年別符号が過半**（≥4/7）。ROI は判定に使わない。
- 満たせば「dev で有望」＝2027 事前登録候補。満たさねば**登録せずクローズ**（目的関数比較・ROI と同じ規律）。

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

## 実装前に確定が要る点（ユーザ査読）

1. **カテゴリ集合と交互作用**：上表でよいか（列数・少数カテゴリ縮約の方針）。
2. **target**：production 既定 `rank`(top3) を primary にするか、`rank_win`(1着) にするか。
3. **同日リーク規約**：発走時刻順で同日内も causal にするか、時刻不明日は「前日まで」に丸めるか。
4. **α（縮約）**：development inner-CV 選定でよいか、固定値にするか。
5. **sire/damsire の materialize**：UKC 由来の父/母父が featured に実在するか（無ければ先に配線＝別作業）。

これらを確定して本書を凍結してから `scripts/run_target_encoding.py` ＋ causal TE builder（純部 tests）を実装する。
実行は development のみ（2025+ fail-closed）・採否は dev で確定しない（有望なら 2027 で一度だけ）。
