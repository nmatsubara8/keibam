# 目的関数比較 実験設計（OBJ_COMPARE・Binary vs LambdaRank vs Race-softmax）

**状態: 設計のみ（未実装）**。着手前に全設計判断を固める（ユーザ指示）。実装は本書を凍結してから。

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
| 特徴 | production 数値特徴 ＋ **レース内 z-score/percentile/1位差/中央値差**（`build_residual_records` 相当）＋ **市場 log-odds `log q`** | ランキング学習の土俵を相対特徴＋市場で揃える |
| 前処理リーク防止 | 特徴は発走時点基準・過去集約は当該走除外・TE は fold 内 OOF・同日後半→前半の逆流禁止 | strictly-prior（既存 asof 規律） |
| 較正 | 全 arm に OOF isotonic を後段適用（较正後で比較） | 目的間の較正差を打ち消して純比較 |
| ハイパラ | 3 目的で共有（num_leaves/lr/min_child 等を固定）。必要なら各目的を **dev 内 inner-CV** で個別 tune し記録 | researcher 自由度の固定 |
| seed | 固定 seed 集合（例 {0,1,2}）で平均・分散を出す | seed 依存の除去 |

## 変える軸（primary factor）＝目的関数 3 種

| arm | LightGBM objective | label | レース内確率化 |
|---|---|---|---|
| **BINARY** | `binary` | rank_win（1着=1） | 予測 p をレース内で再正規化（Σ=1） |
| **LAMBDARANK** | `lambdarank`（group=race_id） | graded relevance（1着=3/2着=2/3着=1/他=0） | score → レース内 softmax |
| **XENDCG(≈softmax)** | `rank_xendcg`（group=race_id） | 同 graded | score → レース内 softmax |

- LightGBM は `binary`/`lambdarank`/`rank_xendcg` を全てサポート＝**同一ライブラリ・同一特徴で目的だけ差し替え**られる（最もクリーンな isolate）。純 listwise softmax が要るなら custom fobj も可だが、まず `rank_xendcg` を softmax 代理とする。
- LAMBDARANK/XENDCG は `group=` に各レースの出走頭数を渡す（レースを query group 化）。

## 副次軸（ablation・primary の後）

1. **±市場特徴**：`log q` を入れる/抜くで、各目的が市場に対しどれだけ純増分を出すか。
2. **±レース内相対変換**：z-score/rank/top差を入れる/抜くで、相対特徴の寄与。
3. **±graded relevance**（1着のみ vs 3着まで加点）。
これらは primary の勝敗が付いてから回す（多重性を primary に持ち込まない）。

## 評価指標（同一 walk-forward OOF で全 arm 横並び）

| 目的 | 指標 |
|---|---|
| 勝率推定・較正 | **LogLoss（レース内正規化・nats/race）**・**ECE**（primary の較正判定） |
| 順位品質 | **レース内 NDCG@1・NDCG@3**（relevance=着順の逆順）・Top-1 accuracy |
| 全順位 | Kendall τ・Plackett–Luce 対数尤度（参考） |
| 収益（**非証拠**） | edge 分位別 ROI：各馬の edge=p_model/q_market で分位化し分位別 ROI（最終単勝・近似）＋venue×日 block bootstrap CI |
| 再現性 | **年別**の LogLoss/NDCG 改善の符号一致（walk-forward 年で過半か） |

NDCG 定義: 1レースを1クエリ、relevance r_i=（着順の逆・例 1着=3,2着=2,3着=1,他0）、DCG=Σ (2^{r_i}−1)/log2(rank+1)、
IDCG で正規化、@k は上位 k。

## 判定規則（selection・事前固定）

**勝者目的の宣言条件**（development walk-forward・非証拠でも selection には十分な規律）:
1. binary に対する **ΔLogLoss（nats/race）の venue×日 block bootstrap paired CI 上限 < 0**（改善が有意）かつ
   **|ΔLogLoss| ≥ MES=0.001**、**ΔECE ≤ +0.005**（較正非悪化）。
2. **年別再現性**：walk-forward 7 年のうち改善符号が**過半**（≥4/7）。
3. NDCG@1/@3 も改善方向（矛盾しない）。
ROI は**判定に使わない**（記述のみ・市場効率の壁）。1/2/3 を満たす目的のみ「dev で有望」と認定。

**多重性**: primary family = {LAMBDARANK vs BINARY, XENDCG vs BINARY} の m=2。Holm(α=0.05)。
副次 ablation は family 外（探索・確認でない）。

## 勝者が出たら（2027 事前登録へ）

dev で有望と認定された目的があれば、**その1つ**（＋固定特徴・ハイパラ・seed・較正）を凍結し、
`run_residual_head_2027` と同型の gated audit→evaluate で 2027 reserved tranche に一度だけ確認する。
B/JRDB42 と 2027 を共有するなら **開封前に family へ一括追加**（Holm 更新）。ROI 採否はさらに別仮説。

## 再利用する既存資産（実装時・再発明しない）

- `build_residual_records`（レース内 z-score・発走前特徴）／`src/training/_temporal_split.rolling_folds`・
  `filter_selection_domain`（development walk-forward・selection 域 fail-closed）。
- `src/simulation/_model_compare.block_bootstrap_ci`（venue×日 paired CI）・`race_nll`/`ece`。
- `src/policies/_market_residual.market_probs`（q=1/O 正規化）・`_residual_head`（race-softmax 参照実装）。
- `src/training/_keiba_ai`／`_model_wrapper`（LightGBM 学習・OOF）・`_calibration`（isotonic/temperature）。
- LightGBM `LGBMRanker`（lambdarank/rank_xendcg・group=race）は新規配線が要る唯一の本質部分。

## スコープ外（別実験）

- **時系列 OOF Target Encoding**（騎手×場・種牡馬×距離）：有望だが feature-engineering の別軸。本比較の
  固定特徴には入れず、勝者目的が決まった後に別 ablation とする。
- **ROI 戦略の採否**：非証拠のまま。控除超過の証拠は 5 経路 null と整合。
- **NN/深層**：表形式で GBDT 優位の前提（ユーザ所見）ゆえ primary には入れない。

## 実装チェックリスト（凍結後に着手）

- [ ] `scripts/run_objective_comparison.py`：3 arm を同一 fold/特徴で学習し上記指標を出力。
- [ ] LGBMRanker 配線（group=race・graded relevance）＋レース内 softmax 確率化。
- [ ] OOF isotonic を全 arm に適用（較正後比較）。
- [ ] ΔLogLoss/ΔNDCG の venue×日 block bootstrap paired CI＋年別符号＋Holm(m=2)。
- [ ] edge 分位別 ROI（非証拠・記述）。
- [ ] 純部の単体テスト（NDCG・relevance 変換・レース内 softmax・fold 分割・判定規則）。
- [ ] 実行は development(2015-2024) のみ（2025+ は fail-closed）。性能の採否は dev では確定しない（2027）。
