# ファンダメンタル模型の「単勝と直交する情報」検証（Benter 型 combining logit）

「単勝オッズと直交する予測情報を、公開データのファンダモデルは持つか？」を中央(JRA)・地方(NAR)
両方で実測する。結論・数値は `docs/fundamental_orthogonality.md`。

## パイプライン

```
python scripts/fundamental_edge/scrape_races.py  --pool jra --n-races 250 --out data/ff/runners_jra.csv
python scripts/fundamental_edge/scrape_races.py  --pool nar --n-races 250 --out data/ff/runners_nar.csv
python scripts/fundamental_edge/scrape_horses.py --runners data/ff/runners_jra.csv data/ff/runners_nar.csv \
    --out data/ff/hist.csv                                  # unique horses の過去走(resumable)
python scripts/fundamental_edge/combining_logit.py --jra-runners data/ff/runners_jra.csv \
    --nar-runners data/ff/runners_nar.csv --hist data/ff/hist.csv
```

- `scrape_races.py`: 出走馬ごとに horse_id/単勝/着順/斤量/馬体重/枠を取得（結果表リンクから horse_id 抽出）。
- `scrape_horses.py`: `horse/result/{id}`(AJAX, Referer必須)から過去走。resumable（既取得はスキップ）。
- `combining_logit.py`: リークセーフなファンダ特徴（**市場由来のオッズ・人気は除外**）で条件付き
  ロジット f を学習 → `u=α·log f + β·log π` を train で当て、test の McFadden **ΔR²** を market-only と比較。

## 結論（各250レース, 2026年）

| | ファンダ単独R² | 市場のみR² | **ΔR²** |
|---|---|---|---|
| JRA（効率市場） | 0.121（≈Benter公開0.1245） | 0.320 | **+0.0009** |
| NAR（部分非効率） | 0.098 | 0.393 | **−0.0055** |

ファンダモデルは勝ち馬を当てる（R²≈Benter同等）が、市場に足すと ΔR²≈0＝**直交情報ゼロ**。
combining は α(ファンダ)≈0 しか与えない＝市場が全部織り込み済み。Benter の ΔR²=0.0178 は
独自特徴×低効率プール由来で、公開特徴×効率市場では再現しない。**公開データ予測 NO-GO を確定。**

## 注意

- 取得データは第三者(netkeiba)由来・ephemeral のため **非コミット**（`data/` は gitignore）。
  数値は doc に保存し、上記スクレイパで再取得する。礼儀: curl 経由・~0.8s 間隔・指数バックオフ。
