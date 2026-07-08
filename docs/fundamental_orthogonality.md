# 単勝と直交するファンダメンタル情報の実測（Benter 型 combining logit の OOS ΔR²）

## 問い（調査全体で唯一残っていた扉）

市場効率の結論（JRA echo 0.989・ΔR²≈0）は「予測 vs 市場の相関」で示してきたが、Benter の
正攻法 — **市場をすでに知っているモデルに、ファンダメンタル予測を足したときの out-of-sample の
増分予測力（combining logit の ΔR²）** — を直接測ってはいなかった。これが公開データ NO-GO を
確定させる最後の未検証項目。「全国・地方に限らず」検証した。

## 方法

- **データ**: netkeiba から中央(JRA)・地方(NAR)各250レースをスクレイプ（2026年）。JRA 3,057頭・
  NAR 2,424頭・計5,481頭の**過去走**（`horse/result/{id}` AJAX）を取得（リークセーフに当該レース
  より前の走のみ使用）。
- **ファンダ特徴（12, すべて非市場・発走前確定）**: 過去走の相対着順(直近5走の平均/最良)・通算
  勝率/複勝率・出走経験(log)・間隔(log)・賞金(log)・斤量・馬体重・年齢・枠。**市場由来のオッズ・
  人気は意図的に除外**（直交性を汚さないため）。
- **モデル**: 条件付きロジット（scipy, レース内 softmax）。
  1. ファンダ単独 f を train で学習。
  2. Benter 型 combining: `u_i = α·log f_i + β·log π_i`（π=単勝含意）を train で学習。
  3. **test（日付で後方30%）** の McFadden R² を market-only と比較。ΔR² = 直交情報。

## 結果

| | ファンダ単独 R² | 市場のみ R² | 市場+ファンダ R² | **ΔR²** | α(ファンダ) | β(市場) |
|---|---|---|---|---|---|---|
| **JRA（中央=効率市場）** | **0.1212** | 0.3204 | 0.3213 | **+0.0009** | −0.034 | +0.924 |
| **NAR（地方=部分非効率）** | 0.0980 | 0.3928 | 0.3874 | **−0.0055** | +0.103 | +0.901 |

（train≈178/189 races, test≈72/60 races）

## 読み取り — 「予測はできるが、市場と直交しない」

1. **ファンダモデルは本物**。JRA単独 McFadden R²=0.121 は **Benter 自身の公開ファンダモデル
   R²=0.1245 とほぼ同一**。過去走等からモデルは実際に勝ち馬を当てている（弱いモデルではない）。
2. **だが市場に足すと ΔR²≈0**。増分は JRA +0.0009 / NAR −0.0055＝ゼロと区別できない。combining
   ロジットは市場を知ると**ファンダに重み ≈0（α=−0.034）**しか与えない。**ファンダが知ることは
   すべて単勝オッズが既に織り込み済み。**
3. **地方でも同じ（むしろ負）**。「部分非効率」の NAR ですら ΔR² 負（OOS で過学習して悪化）。

## Benter との対比が全てを説明する

| | ファンダ R² | combined − public ΔR² |
|---|---|---|
| Benter（香港・独自加工特徴・10人年） | 0.1245 | **+0.0178**（黒字化の源泉） |
| 本実測（公開特徴・JRA） | 0.121（≈同等） | **+0.0009**（約1/20） |

モデルの質はほぼ同じ。違いは **特徴が独自か公開か × プールが低効率か効率か** だけ。Benter の
ΔR²=0.0178 は、市場が過小評価する独自特徴を低効率な香港プールで使ったから生まれた。**公開特徴
×効率的な JRA/NAR では、市場が全部織り込み済みで直交情報はゼロ**になる。これを直接実測で示した。

## 位置づけ（調査の完全な閉包）

| 市場 | 券種 | 予測器 | 結果 |
|---|---|---|---|
| JRA | 単勝 | 市場echo | echo 0.989・ΔR²≈0 |
| JRA | — | **ファンダ(直交)** | **ΔR² +0.0009**（本実測） |
| NAR | 単勝 | 市場FLB | 本命0.86 < 控除20% |
| NAR | 連系 | Harville/割引/市場LL | 市場が最良予測器（`docs/nar_efficiency_pilot.md`） |
| NAR | — | **ファンダ(直交)** | **ΔR² −0.0055**（本実測） |
| 両 | — | リベート | ≤10% < 控除率 |

**公開データで JRA/NAR の市場を予測で出し抜く扉は、すべて閉じている。** 唯一の合理的最適化は
損失最小化（`docs/loss_minimization_design.md`）。

## 正直な限界

- 各250レース・test≈60-72レース（ΔR² には標本ノイズ。ただし α重み≈0 と両プール一致が結論を支える）。
- ファンダ特徴は標準的な公開特徴（ペース指数・血統・騎手/調教師フォーム・馬場バイアスの精緻な
  工学は未実装）。より richな特徴で ΔR² が僅かに動く余地はあるが、(a) 既往の300+特徴/pace 検証でも
  echo 0.989、(b) Benter の edge は独自特徴×低効率プール由来、が見込みの薄さを示す。
- データは第三者(netkeiba)由来・ephemeral ゆえ非コミット（`data/` は gitignore）。数値は本 doc に保存。

## 再現手順

`scripts/fundamental_edge/`（データ再取得を含む）:

```
python scripts/fundamental_edge/scrape_races.py  --pool jra --n-races 250 --out data/ff/runners_jra.csv
python scripts/fundamental_edge/scrape_races.py  --pool nar --n-races 250 --out data/ff/runners_nar.csv
python scripts/fundamental_edge/scrape_horses.py --runners data/ff/runners_jra.csv data/ff/runners_nar.csv --out data/ff/hist.csv
python scripts/fundamental_edge/combining_logit.py --jra-runners data/ff/runners_jra.csv \
    --nar-runners data/ff/runners_nar.csv --hist data/ff/hist.csv
```
