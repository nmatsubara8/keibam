# 目的関数比較 実験設計（OBJ_COMPARE・Binary / Ranking / Race-softmax CE）

**状態: 凍結（2026-08-02・ユーザ条件付き承認の3修正を反映）→ 実装可**。以後この仕様を結果を見て変えない。

## 目的（1つの問い）

> 表形式競馬データで、**目的関数をランキング/リストワイズに変える**と、現行の pointwise binary GBDT より
> レース内の勝率推定（LogLoss/較正）と順位品質（NDCG）が **development で一貫して改善するか**。

これは **モデル選択**であり standing protocol の **selection（development_known 2015-2024）で実施可・reserved
tranche 非消費**（`assert_selection_only_on_known`）。勝者が出たら 2027 用に**別途事前登録**する。ROI は
non-evidential（市場半強効率の作業帰無：回収率≈控除後）。既存 B/JRDB42 の凍結仕様には触れない。

## 固定する軸（交絡を避けるため全 arm で共通）

| 項目 | 固定値 | 根拠 |
|---|---|---|
| モデルクラス | **LightGBM**（3 目的とも同一実装） | 目的関数だけを isolate（NN/線形と混ぜない） |
| fold | purged walk-forward：train=[2015,Y) → valid=Y, Y∈2018..2024（`rolling_folds`） | development 内・時系列 OOF |
| 特徴 | production 数値特徴 ＋ **レース内 z-score/percentile/1位差/中央値差**（`build_residual_records` 相当）＋ **市場 log-odds `log q`（＝market-informed 特徴）** | ランキング学習の土俵を相対特徴＋市場で揃える |
| 前処理リーク防止 | 特徴は発走時点基準・過去集約は当該走除外・TE は fold 内 OOF・同日後半→前半の逆流禁止 | strictly-prior（既存 asof 規律） |
| 較正（確率化） | ranker arm は **fold 内 training-OOF から単一 temperature T>0 を推定**し `p=softmax(s/T)`。馬単位 isotonic→再正規化は**不採用** | レース内 simplex を直接較正・スコア任意スケール問題を1自由度で解消・arm 間の自由度を揃える |
| ハイパラ | 3 目的で共有（num_leaves/lr/min_child 等を固定）。必要なら各目的を **dev 内 inner-CV** で個別 tune し記録 | researcher 自由度の固定 |
| seed | 固定 seed 集合（例 {0,1,2}）で平均・分散を出す | seed 依存の除去 |

## label（全 arm 共通・estimand を揃える）＝ winner-only 二値

**全 arm で `winner=1 / others=0`（1着のみ二値）に固定**。graded relevance（1着=3/2着=2/3着=1）は
LambdaRank だけが「複勝圏を含む順位品質」を学習して estimand が変わり **目的関数の isolate にならない**ため
primary に入れない（別の secondary exploratory とする）。全 arm が「勝者確率」を推定する同一 estimand。

## arm 一覧（4 本＋任意1本）

| arm | 実装 | 種別 | レース内確率化 |
|---|---|---|---|
| **BINARY** | LightGBM `objective=binary`・label=winner-only | pointwise | 予測 p をレース内で再正規化（Σ=1） |
| **LAMBDARANK** | LGBMRanker `objective=lambdarank`・group=race・winner-only relevance | ranking(pairwise) | score → **OOF temperature** softmax |
| **XENDCG** | LGBMRanker `objective=rank_xendcg`(=XE_NDCG_MART)・group=race・winner-only | ranking | score → **OOF temperature** softmax |
| **RACE_SOFTMAX_CE** | LightGBM **custom listwise objective**（下記）・group=race | **listwise softmax CE** | 直接 p（レース内 softmax） |
| （任意）MARKET_ANCHORED_CE | custom `p=softmax(log q + f(x))`（係数1で `log q` を anchor） | listwise・market-anchored | 直接 p |

- `rank_xendcg` は **XE_NDCG_MART＝ランキング目的**であり **race-softmax CE ではない**（LightGBM 公式定義）。
  代理にしない。一次評価がレース内 LogLoss である以上、それを**直接最適化する純 listwise arm**
  （RACE_SOFTMAX_CE）が無いと最重要仮説を検証できない。
- 任意 arm 5 は `log q` を**係数1で固定 anchor**する（＝market-anchored・既存 residual head の再確認）。
  arm 1-4 の `log q` は**係数を学習する market-informed 特徴**であり anchor ではない（呼称を区別）。

### RACE_SOFTMAX_CE の custom objective（LightGBM）

各レース r の出走馬スコア s から `p_{ir}=softmax_{j∈r}(s_{jr})`、損失 `L_r=−log p_{winner,r}`。
勾配・対角 Hessian は

    g_i = p_i − y_i      （y_i: 1着=1 それ以外0）
    h_i = p_i (1 − p_i)

を LightGBM の custom `fobj(preds, dataset)` に配線（`group=` でレース境界を渡し race 内 softmax を作る）。
純部（softmax・grad・hess・group 分割）は単体テストする。

## 副次軸（ablation・primary の後・family 外）

1. **±市場特徴**：`log q` を入れる/抜くで各目的の市場に対する純増分。
2. **±レース内相対変換**：z-score/rank/top差の寄与。
3. **graded relevance**（複勝圏順位品質・exploratory・採否外）。
4. **MARKET_ANCHORED_CE**：既存 residual head の GBDT 版再確認（secondary）。
primary の勝敗が付いてから回す（多重性を primary に持ち込まない）。

## 評価指標（同一 walk-forward OOF で全 arm 横並び）

| 目的 | 指標 |
|---|---|
| 勝率推定・較正 | **LogLoss（レース内正規化・nats/race）**・**ECE**（primary の較正判定） |
| 順位品質 | **レース内 NDCG@1・NDCG@3**（relevance=着順の逆順）・Top-1 accuracy |
| 全順位 | Kendall τ・Plackett–Luce 対数尤度（参考） |
| 収益（**非証拠**） | edge 分位別 ROI：各馬の edge=p_model/q_market で分位化し分位別 ROI（最終単勝・近似）＋venue×日 block bootstrap CI |
| 再現性 | **年別**の LogLoss/NDCG 改善の符号一致（walk-forward 年で過半か） |

NDCG 定義: 1レースを1クエリ、relevance r_i=（着順の逆・例 1着=3,2着=2,3着=1,他0）、DCG=Σ (2^{r_i}−1)/log2(rank+1)、
IDCG で正規化、@k は上位 k。（NDCG は順位品質の**参考**指標であり primary 判定は LogLoss。）

**確率の2段報告**（arm 間で明快に比較する）:

| 出力 | 用途 |
|---|---|
| 未較正・canonical 変換（binary=そのまま/ranker=素 softmax T=1） | objective そのものの性能（順位品質） |
| **OOF temperature 較正後**（ranker は T>0 推定、softmax_ce/binary は恒等 or 同じ T 手順） | 実用上の確率性能・**primary 判定（LogLoss/ECE）** |

## 仮説構造と判定規則（selection・事前固定）

primary 評価が **レース内 LogLoss** である以上、それを直接最適化する **RACE_SOFTMAX_CE を主要 challenger**に
据える（検出力最大化）:

- **PRIMARY（m=1）**: `RACE_SOFTMAX_CE − BINARY` の ΔLogLoss。
- **SECONDARY family（Holm m=2）**: `LAMBDARANK − BINARY`、`XENDCG − BINARY` の ΔLogLoss。
- **exploratory（family 外）**: graded relevance・MARKET_ANCHORED_CE・±市場/±相対変換の ablation。

**勝者宣言条件**（各対比較）:
1. **ΔLogLoss（nats/race）の venue×日 block bootstrap paired CI 上限 < 0**（改善が有意）かつ
   **|ΔLogLoss| ≥ MES=0.001**、**ΔECE ≤ +0.005**（較正非悪化）。全て **OOF temperature 較正後**の確率で。
2. **年別再現性**：walk-forward 7 年のうち改善符号が**過半**（≥4/7）。
3. NDCG@1/@3 が矛盾方向でない（参考）。
PRIMARY は m=1 でそのまま、SECONDARY は Holm(α=0.05, m=2)。ROI は**判定に使わない**（記述のみ・
市場効率の壁）。条件を満たす目的のみ「dev で有望」と認定。

## 勝者が出たら（2027 事前登録へ）

dev で有望と認定された目的があれば、**その1つ**（＋固定特徴・ハイパラ・seed・較正）を凍結し、
`run_residual_head_2027` と同型の gated audit→evaluate で 2027 reserved tranche に一度だけ確認する。
B/JRDB42 と 2027 を共有するなら **開封前に family へ一括追加**（Holm 更新）。ROI 採否はさらに別仮説。

## 再利用する既存資産（実装時・再発明しない）

- `build_residual_records`（レース内 z-score・発走前特徴）／`src/training/_temporal_split.rolling_folds`・
  `filter_selection_domain`（development walk-forward・selection 域 fail-closed）。
- `src/simulation/_model_compare.block_bootstrap_ci`（venue×日 paired CI）・`race_nll`/`ece`。
- `src/policies/_market_residual.market_probs`（q=1/O 正規化）・`_residual_head`（race-softmax 参照実装）。
- `src/training/_keiba_ai`／`_model_wrapper`（LightGBM 学習・OOF）・`_calibration`（temperature scaling）。
- **新規配線が要る本質部分**: (a) `LGBMRanker`（lambdarank/rank_xendcg・group=race）、(b) **RACE_SOFTMAX_CE
  の custom `fobj`**（レース内 softmax・g=p−y・h=p(1−p)）、(c) ranker score の **OOF temperature** 推定。

## スコープ外（別実験）

- **時系列 OOF Target Encoding**（騎手×場・種牡馬×距離）：有望だが feature-engineering の別軸。本比較の
  固定特徴には入れず、勝者目的が決まった後に別 ablation とする。
- **ROI 戦略の採否**：非証拠のまま。控除超過の証拠は 5 経路 null と整合。
- **NN/深層**：表形式で GBDT 優位の前提（ユーザ所見）ゆえ primary には入れない。

## 実装チェックリスト（凍結済み・着手可）

- [ ] `scripts/run_objective_comparison.py`：4 arm（BINARY/LAMBDARANK/XENDCG/RACE_SOFTMAX_CE）を
      同一 fold/特徴/label(winner-only) で学習し指標を出力。任意で MARKET_ANCHORED_CE。
- [ ] RACE_SOFTMAX_CE の custom `fobj`（レース内 softmax・g=p−y・h=p(1−p)・group 境界）を純関数で実装。
- [ ] LGBMRanker 配線（group=race・winner-only relevance）＋ranker score の **OOF temperature** 推定。
- [ ] 確率2段報告（未較正 canonical ／ OOF temperature 較正後）。primary 判定は較正後 LogLoss/ECE。
- [ ] レース内 NDCG@1/@3（参考）・edge 分位別 ROI（非証拠・記述）。
- [ ] PRIMARY=`RACE_SOFTMAX_CE−BINARY`(m=1)、SECONDARY=`{LAMBDARANK,XENDCG}−BINARY`(Holm m=2) の
      ΔLogLoss venue×日 block bootstrap paired CI＋年別符号。
- [ ] 純部の単体テスト（softmax/grad/hess・temperature 推定・NDCG・fold 分割・判定規則）。
- [ ] 実行は development(2015-2024) のみ（2025+ は `filter_selection_domain` で fail-closed）。採否は dev で
      確定しない＝有望目的が出たら features/objective/hyperparam/seed/温度手順を凍結し 2027 で一度だけ確認。
