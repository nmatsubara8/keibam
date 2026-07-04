# ライブ推論の person_te / form train-serve skew — 調査と設計

## 調査結果（根本原因）

学習(featured)は `DataMerger` が **results 履歴**（全レース結果）から集計特徴を作る。一方ライブ予測は
`ShutubaDataMerger`（`predict_upcoming.py:280`）で、`merge()` が呼ぶのは:
`race_info_shutuba → horse_results → horse_info → peds → live_ratings(Elo snapshot)` のみ。

**`ShutubaDataMerger` は履歴 results を一切渡されていない**（コンストラクタは shutuba 表・horse_results・
horse_info・peds・race_info のみ）。このため **results 履歴由来の特徴が serve で再現されない**:

| 特徴ブロック | train の出所 | serve での状態 |
|---|---|---|
| **person_te**（jockey/trainer×context 勝率）※本作業で追加 | `_merge_person_target_encoding`（results 履歴） | **未計算 → 0 埋め** |
| person_yearly（jockey_py_* 前年集計） | `_merge_person_yearly` | shutuba merge() が呼ばない → **0 埋め** |
| jockey/trainer/owner 直近 rolling（`_attach_jockey_trainer_stats`） | `_merge_horse_results` 内・self._results(=履歴)の着順 | serve は self._results=出馬表(着順NaN) → **NaN→0** |
| **form-from-results**（未取得馬の率系 form）※本作業で追加 | `build_horse_results_from_results(self._results=履歴)` | serve は self._results=出馬表(着順なし) → **空→0** |

モデルの `calc_score` は `X.reindex(columns=feat_cols, fill_value=0)` で**不足列を 0 埋め**し警告を出すだけ
（`_keiba_ai.py:295+`）。よって serve は**エラーにならず静かに skew**する。

**含意**: ライブ予測は事実上「**騎手/調教師/馬主・person 系特徴を全て 0**、未取得馬の率系 form も 0」で
走っている。market/Elo/yoso/血統/取得済み馬の form には効くが、person シグナルと再構成 form が欠落。
（market 併走なので致命的な誤りにはならないが、学習した person/form の寄与が serve で失われる skew。）

## 設計（スナップショット＝再計算方式・train 関数を再利用して skew ゼロ）

Elo が `features_from_snapshot` で serve を再現するのと同型。person_te / form は純粋な集計なので、
**serve 時に履歴 results を読み、train と同じ関数で出走馬分を再計算**するのが最も安全（保存物の不整合が無い）。

### 変更点
1. **履歴 results を注入**: `ShutubaDataMerger` に `history_results`（`load_raw(RAW_RESULTS_PATH)`）を渡す。
   `predict_upcoming` の merger 構築時に追加。
2. **form-from-results（serve）**: `_merge_horse_results` の再構成を **`self._results` ではなく
   `history_results`** から行う（train は history=None → self._results＝履歴で従来通り）。
   → `build_horse_results_from_results(history_results)` を scraped horse_results に union → 既存の
   date スライス（date < 発走日）で出走馬の過去走に絞られ、率系 form が計算される。
3. **person_te（serve）**: `concat([history_results(着順あり), 出馬表(着順=NaN, date=発走日)])` に
   `build_person_form_features` を適用し、**出馬表行の encoding だけ**取り出して self._results に付与。
   expanding は厳密過去なので、出走馬行は「発走日より前の全履歴」の as-of 値になる（train と同一計算）。
   出馬表に無い entity/context(owner_id 等)の spec は自動スキップ。
4. **同根の既存 skew も同時に是正**（推奨）: `_merge_person_yearly` を serve でも呼ぶ、
   `_attach_jockey_trainer_stats` を history から計算する（同じ history 注入で対応可能）。

### 擬似コード（ShutubaDataMerger）
```python
def merge(self):
    ...
    self._merge_horse_results()            # ← 内部で history から form-from-results union
    self._merge_person_target_encoding_shutuba(self._history_results)  # 新規
    self._merge_horse_info(); self._merge_peds(); self._merge_live_ratings()

def _merge_person_target_encoding_shutuba(self, history):
    if history is None or history.empty: return
    up = self._results.copy(); up["着順"] = pd.NA; up["date"] = self._race_date
    combined = pd.concat([history[needed_cols], up[needed_cols]], ignore_index=False)
    feats = build_person_form_features(combined, alpha=KEIBA_TE_ALPHA)
    up_feats = feats.iloc[-len(up):]          # 出馬表行だけ
    for c in up_feats.columns: self._results[c] = up_feats[c].to_numpy()
```

## 検証プロトコル（skew ゼロの確認）

1. **過去レースで一致テスト**: 既に featured にある過去レース R を選び、その発走前状態を模して serve 経路で
   person_te / form を計算 → **featured[R] の同名列と値が一致**することを確認（許容誤差内）。一致すれば
   train/serve skew 解消。
2. 単体テスト: `_merge_person_target_encoding_shutuba` が出馬表行に as-of encoding を付け、
   history の未来行を混ぜないこと（既存 `expanding_target_encode` の厳密過去テストで担保済み）。
3. `predict_upcoming` の警告ログ「N 列が X に存在しないため 0 で補完」から person_te/form 列が消えること。

## 優先度と段取り
- P1: history 注入 ＋ person_te(serve) ＋ form-from-results(serve)（本作業の (C) 直結）。
- P2: person_yearly / jockey-trainer-recent の serve 再現（同じ history 注入で対応・skew 完全解消）。
- 実装後、直近レースで `predict_upcoming` を回し、person_te/form 列が 0 でなく妥当値になることを確認。
