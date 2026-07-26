# 荒れ度×配当 と bold-play の数値地図

「常勝」でなく **(a) どの条件で高配当が出やすいか / (b) 一撃狙いの的中率・配当分布** を中央/地方で
数値化する。結論・数値は `docs/payout_map.md`。edge は主張しない。

## パイプライン

```
python scripts/payout_map/scrape_pool.py --pool jra --n-races 250 --out data/payout_map/jra
python scripts/payout_map/scrape_pool.py --pool nar --n-races 250 --out data/payout_map/nar
python scripts/payout_map/analyze_payout_map.py --label 中央(JRA) \
    --runners data/payout_map/jra_runners.csv --payoffs data/payout_map/jra_payoffs.csv
python scripts/payout_map/analyze_payout_map.py --label 地方(NAR) \
    --runners data/payout_map/nar_runners.csv --payoffs data/payout_map/nar_payoffs.csv
```

- `scrape_pool.py`: プール別に1フェッチで単勝/着順＋三連単払戻を取得（`{out}_runners.csv` / `{out}_payoffs.csv`）。
- `analyze_payout_map.py`: (a) 荒れ度(エントロピー)×配当、(b) ターゲット別の的中率/回収率/配当分布、
  bold-play の目標到達確率（stake の大胆さ）。

## 結論（要約）

- 荒れ度は配当を強く予測（corr 0.31–0.39）だが織り込み済で edge 無し。JRA は配当が中央値で2〜3倍大きい。
- 全戦略で回収率<1.0（平均マイナス）。深い穴の狙い撃ちは的中ほぼ0。≥100倍は BOX/フォーメーションから稀に出る。
- 一撃狙いは bold play（本命/BOX の大胆 staking）が両プールで機能し JRA の方が届きやすい。EV は常にマイナス＝分散の設計。

## 注意

取得データは第三者(netkeiba)由来・ephemeral のため **非コミット**（`data/` は gitignore）。数値は doc に保存。礼儀: curl 経由・~0.8s 間隔。
