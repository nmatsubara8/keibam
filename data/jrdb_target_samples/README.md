# JRDB TARGET 追加データ サンプル置き場

B フェーズ(配布データ現物調査)で取得したサンプルを **ここに置く**。中身(`.zip` / `.DAT` / `.txt`)は
`.gitignore` 済み(有料データ・コミット禁止)。この README と `.gitkeep` のみ追跡される。

## 置き方

系列ごとにサブディレクトリを切ると `scripts/jrdb_target_profile.py` の出力が読みやすい。

```
data/jrdb_target_samples/
  gaikyu_comment/      外厩コメント   ← 最優先
  gaikyu_mark/         外厩馬印
  tenkai_mark/         展開馬印
  bante_mark/          番手馬印
  idm_mark/            IDM馬印
  result_idm/          成績IDM
  trainer_rank/        厩舎ランク
  jockey_rank/         騎手ランク
```

各系列に **日次1ファイル＋年次1ファイル**(2016年と2025/2026年から各1)を入れれば、URL規則・形式・年次差は
ほぼ判定できる。

## プロファイル実行

```
python scripts/jrdb_target_profile.py
# → docs/jrdb_target_file_profiles.json（.gitignore 済み）に自動判定結果
```

## 確定済みの結合キー(外厩コメント左辺 10桁)

`0126120103` = `race_key(8)` + `umaban(2)`。既存 `src/jrdb/_keys.py` の変換がそのまま使える:

```python
from src.jrdb._keys import race_key_to_race_id
race_id = race_key_to_race_id(key[:8])   # 日は16進1桁・世紀補完込み
umaban  = int(key[8:10])
```

注意: `race_key` の「日」は **16進1桁**(10〜15日目 = a〜f)。左辺を 10進で桁分割しないこと。
