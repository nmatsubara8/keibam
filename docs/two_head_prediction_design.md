# 2ヘッド予測アーキテクチャ 詳細設計（Place=複勝 / Win=単勝）

最終更新: 2026-06-23 / ブランチ `claude/kind-meitner-1yqyzu`

## 0. TL;DR
- 現行モデルの目的変数 `rank` は **着順<4＝複勝(top3)** だった。これを Harville に「勝率」として
  渡しており、**単勝・連系の確率が近似（不整合）** だった。
- 解決策＝**2ヘッド**:
  - **Place ヘッド**（既存・target=`rank`=top3）→ 複勝/ワイドの的中確率を**直接**供給。
  - **Win ヘッド**（新規・target=`rank_win`=1着）→ Harville に**真の勝率**を供給し、
    単勝/馬連/馬単/三連複/三連単/枠連を正す。
- 後方互換: Win ヘッドが無い旧モデル・`--no-win-head` 運用では従来挙動のまま。

---

## 1. 背景：何が問題だったか

```
ResultsProcessor: df["rank"] = (着順 < 4)   # ← top3（複勝）であって 1着ではない
        │
        ▼
学習: target = rank(top3)  →  model.predict_proba = P(top3)
        │
        ▼
EV選定: win_probs = normalize(P(top3))      # ← top3確率を「勝率」として正規化（近似）
        harville.combo_probability(...)      # 単勝/連系を win_probs から導出
```

- `P(top3)` を総和で割って「勝率シェア」に変換しているため、単勝・連系の確率は**近似**。
- 複勝に至っては、モデルが直接出している `P(top3)` を捨てて Harville の `prob_place` で
  **再導出**していた（二度手間 + 近似）。

---

## 2. アーキテクチャ全体像

```
                         featured_data（rank, rank_win を両方保持）
                                  │
              ┌───────────────────┴────────────────────┐
              ▼                                          ▼
   Place ヘッド (target=rank=top3)            Win ヘッド (target=rank_win=1着)
   models/<date>/<version>.pickle            models/<date>/<version>__win.pickle
   P_place(h) = P(h が3着内)                  P_win(h)  = P(h が1着)
              │                                          │
              ▼                                          ▼
        ┌─────────────┐                          ┌──────────────┐
        │ 複勝 EV      │ ← P_place 直接           │ 単勝/連系 EV │ ← Harville(P_win)
        │ (Stage A)   │                          │ (Stage B)    │
        └─────────────┘                          └──────────────┘
```

両ヘッドは **同じ featured_data・同じ学習パイプライン**（スタッキング+較正）を
`target_col` だけ差し替えて再利用する。

---

## 3. データフロー（4段）

### 3.1 ラベル生成（preprocessing）
`src/preprocessing/_results_processor.py`
```python
df["rank"]     = 着順 < 4   # Place（複勝/top3）
df["rank_win"] = 着順 == 1  # Win（単勝/1着）
```
`_select_columns` で両列を featured に残す。

### 3.2 学習（training）
`DataSplitter(featured, test_size, valid_size, target_col="rank"|"rank_win")`
- `target_col` で y_train/y_test/y_base/y_meta/y_calib を切替え。
- **リーク遮断**: `_DROP_FOR_TRAIN/_TEST/_PREDICT` に `rank` と `rank_win` を**両方**含める
  （top3 には win が含まれるため、相互リークを必ず防ぐ）。

`KeibaAIFactory.create(featured, target_col=...)` → `RetrainJob` が2回学習:
```
place_ai = create(target_col="rank")     → train → save(vname, suffix="")
win_ai   = create(target_col="rank_win") → train → save(vname, suffix="__win")
```
- Win ヘッドは `with_tuning=False`（Optuna 再探索しない＝コスト2倍化を回避）。
- Win ヘッド学習失敗は **non-fatal**（Place 本体は必ず残る）。
- `retrain --no-win-head` で Win ヘッドを無効化可能（`RetrainConfig.train_win_head`）。

### 3.3 永続化（persistence）
- Place: `models/<yyyymmdd>/<version>.pickle`（従来通り。`_keibam.pickle` 接尾で UI が認識）
- Win:   `models/<yyyymmdd>/<version>__win.pickle`（`find_model_paths` の `_keibam.pickle`
  判定に**一致しない**ため、Place と取り違えない）
- メタ: `meta["win_head"] = {"auc_test": ...}` を記録。

### 3.4 予測・EV選定（app / policies）
`app/_data_loader.py`
```python
load_win_head_for(place_path)  # place の隣の <version>__win.pickle を読む（無ければ None）
```
`app/_prediction_service.run_prediction(model, X, ..., win_model=None)`
```python
place_table = calc(model, X)                 # P(top3)
win_table   = calc(win_model, X)             # P(1着)（win_model 指定時のみ）
policy.select(win_table, place_prob_table=place_table)   # 2ヘッド
# win_model=None なら従来: policy.select(place_table)
```
`src/policies/_bet_policy.py` `ExpectedValueBetPolicy`
```python
for combo in ...:
    if direct_place_prob and bet_type == FUKUSHO:
        prob = place_probs[combo[0]]         # Stage A: 複勝は直接
    else:
        prob = harville.combo_probability(bet_type, win_probs, combo)  # 連系は Harville(勝率)
```

---

## 4. リーク安全性
- `rank` / `rank_win` は学習入力から**常に両方**除外（_DROP_* 3経路 + score_policy）。
- 確定オッズ（単勝）は従来通り学習入力から除外、EV用に X_test に残す。
- 市場歪み特徴（別途実装）も確定オッズ由来でリーク無し。

---

## 5. 影響ファイル一覧
| ファイル | 変更 |
|---|---|
| `preprocessing/_results_processor.py` | `rank_win` 生成・列選択追加 |
| `training/_data_splitter.py` | `target_col` 引数化・両ラベルを drop |
| `policies/_score_policy.py` | `_DROP_FOR_PREDICT` に rank_win |
| `policies/_bet_policy.py` | 複勝直接 + `place_prob_table` 受口 + `direct_place_prob` |
| `training/_keiba_ai_factory.py` | `create(target_col)` / `save(suffix)` |
| `pipeline/_retrain.py` | `train_win_head` + Win ヘッド学習/保存(非致命) |
| `pipeline/run_pipeline.py` | `retrain --no-win-head` |
| `app/_data_loader.py` | `win_head_path_for` / `load_win_head_for` |
| `app/_prediction_service.py` | `run_prediction(win_model=)` ルーティング |

テスト: rank_win生成3 / target_col・両ラベルdrop / StageA直接3 / Winヘッド学習2 / path導出。

---

## 6. 後方互換
- Win ヘッド未学習の旧モデル → `load_win_head_for` が None → `run_prediction` は従来挙動。
- `target_col` 非対応の旧 factory（テストスタブ等）→ Win ヘッドを安全にスキップ。
- `direct_place_prob=False` で複勝も従来 Harville 経路に戻せる。

---

## 7. 既知の限界・未対応
1. ~~**ワイドは依然 Harville**~~ → **対応済み(2026-06-23)**: ワイドは Place の top3 marginal から
   **固定サイズ抽出の Hájek 二次近似**で joint を作る（`harville.prob_wide_from_place`）。
   `π_ab ≈ p_a p_b (1 − (1−p_a)(1−p_b)/d), d=Σ p_k(1−p_k)`。3枠制約の負相関を再現し独立積より
   小さくなる。`direct_place_prob=False` で従来 Harville(Plackett-Luce) に戻せる。
2. **未検証**: 本実装コンテナに lightgbm が無く、retrain/predict の実走は未確認。
   ユーザ環境での実走検証が必要（§8）。
3. Win ヘッド学習で**学習時間が約2倍**（同パイプラインを2回）。`--no-win-head` で回避可。
4. ワイドの Hájek 近似は二次近似のため、ペア包含確率の総和は厳密に C(3,2)=3 にはならない
   （概ね一致）。EV 閾値で吸収する前提。

---

## 8. 検証手順（ユーザ環境）
```bash
git pull origin claude/kind-meitner-1yqyzu

# 2ヘッド学習
python -m src.pipeline.run_pipeline retrain --version-name twohead_$(date +%Y%m%d)
ls -la models/$(date +%Y%m%d)/        # <version>.pickle と <version>__win.pickle

# 学習層テスト（lightgbm 必要）
python -m pytest tests/training/test_data_splitter_leakage.py tests/pipeline/test_retrain.py -q
```
ログに `Win ヘッド保存: <version>__win auc_test=...` が出れば成功。
Place(複勝=top3) と Win(単勝=1着) で AUC・正例率が異なるはず（Win は正例少で難）。

---

## 9. 次の選択肢
- (A) 2ヘッドを **10年データで本学習** → importance/AUC 比較。
- (B) **ワイドも Place ベース**に（joint 近似 or 直接モデル化）。
- (C) market_signals 用に `fetch-final-odds --from-results` で**券種別オッズ取得**を開始。
