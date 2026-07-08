# Optuna + アンサンブル最強構成の OOS 実測ランブック（ローカル）

「Optuna チューニング＋4モデル・スタッキングの最強構成でも、公開データで市場を出し抜けるか」を
**時系列 OOS（walk-forward）**で直接測る手順。データ（JRA-VAN アーカイブ CSV）がある**ローカルマシン**で実行する。

> 背景: これまでの NO-GO（回収率0.685 / echo 0.989 / ΔR²≈0）は **`with_tuning=False`・単一LightGBM** で
> 得たもの。本ランブックはその空白（Optuna＋アンサンブルの OOS）を塞ぐ。

## 0. 準備

```
git pull                      # --with-tuning / --tuning-config / ingest_archive を取得
pip install -r requirements.txt   # optuna, lightgbm, xgboost, catboost, torch(nn) が要る
```

## 1. アーカイブ CSV → featured_data

```
python seed_from_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv"
python build_seed_featured.py
cp data/raw/seed_featured_data.pkl data/raw/featured_data.pkl
```

またはアップロード運用なら `python scripts/ingest_archive.py`（uploads から自動検出して上記3手を実行）。

**まず小さく通す**（配線確認・数分）:
```
python seed_from_csv.py <csv> --limit 200000 && python build_seed_featured.py && \
  cp data/raw/seed_featured_data.pkl data/raw/featured_data.pkl
```

## 2. 最強構成で OOS 実測

> **重要**: `--stacking` 単体は base 学習器が **LightGBM のみ**（既定 `models=("lightgbm",)`）。
> 多モデル・アンサンブルにするには **`--base-models` で顔ぶれを明示**する必要がある。
> `kernel` = Random Fourier Features + ロジ回帰（大規模で不可な厳密カーネルの線形時間近似）。

### (A) 対市場 ΔR²（本命の判定）
```
python walk_forward.py --quality --stacking --with-tuning \
    --base-models lightgbm,xgboost,catboost,nn,kernel --tune-per-model \
    --tuning-config configs/tuning_config.example.json --folds 5
```
- 各 fold: 過去のみで **5モデル（GBDT×3＋NN＋カーネル）×Optuna→スタッキング**を学習 → 直前 fold で
  合成 (α,β) を fit → 評価 fold で **市場 / モデル / companion（α·log f＋β·log π）** の OOS
  logloss・Brier・ECE を集計。
- **判定**: 通算プールで **companion が「市場」を logloss・ECE ともに安定して下回れば edge**
  （market-companion 達成）。下回らなければ **チューニング＋アンサンブル＋カーネル込みでも NO-GO** を確定。

### (B) OOS 回収率（オッズ帯別）
```
python walk_forward.py --stacking --with-tuning \
    --base-models lightgbm,xgboost,catboost,nn,kernel --tune-per-model \
    --tuning-config configs/tuning_config.example.json --by-odds --folds 5
```

> まず軽く通すなら base を絞る: `--base-models lightgbm,kernel`（カーネルの寄与だけ見たいとき）。
- 単勝の OOS 回収率を全 fold プールで算出。1.0 を安定超えなければ黒字化せず。

## 3. 探索設定（`--tuning-config`）

| ファイル | 中身 |
|---|---|
| （無指定） | LightGBMTuner の自動段階探索（手軽・LightGBMのみ） |
| `configs/tuning_config.example.json` | Optuna・**100試行**・8パラメータ（標準・まずこれ） |
| `configs/tuning_config.max.json` | より広い探索（重い） |
| `configs/tuning_config.regularized.json` | 正則化強めの探索 |

`n_trials` / `timeout` / `search_space` は JSON で調整可。nn/xgb/catboost 側の探索は
`base_models_config`（`_base_models_config.py`）で制御。

## 4. 注意

- **重い**: Optuna 100試行 × 4モデル × 各 fold。CPU では数十分〜数時間。まず `--limit` 小サンプル＋
  `--folds 3` で通し、問題なければ全量・`--folds 5+`。
- **リーク厳禁**: 目的変数 `rank_win` は特徴から DROP 済み（設計上クリーン）。walk-forward は時系列で
  train<eval を保証。もし OOS 回収率が突如 1.0 を大きく超えたら、まずリークを疑う
  （`python debug_leak.py` で held-out 単独 AUC を確認）。
- **結果の読み方**: 効くのは絶対 AUC/logloss でなく **companion − 市場**（＝市場が織り込んでいない情報）。
  絶対性能が上がっても companion≈市場なら edge ではない。

## 5. 事前の見込み（実測で覆すためのベースライン）

echo 0.989・ファンダ combining ΔR²=+0.0009・「連系プールが割引Harvilleより賢い」から、
**チューニング＋アンサンブルでも companion ≈ 市場（NO-GO）**が見込み。ΔR² は特徴 vs 市場の
直交性で決まり、fit 品質（チューニング）や統合法（アンサンブル）では動きにくいため。
**本ランブックはこの"見込み"を"実測"に変えるためのもの。** 結果を持ち寄れば一緒に解釈する。
