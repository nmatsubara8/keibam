---
title: "予測モデルの性能評価指標"
category: concept
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0064
  - sem_0065
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "2713s – 2785s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=2713"
---

予測モデルの性能は、以下の指標で評価できます。

* **的中率 (Hit Rate):** 予測が正しかった割合。
* **回収率 (Return Rate):** 投資額に対するリターン率。
* **回収率の偏差 (Deviation of Return Rate):** 回収率のばらつき。

**事例:** トップ1（全てのレースで上位1頭の単勝を買う場合）、的中率は32.8%、回収率は約84%となる。

**改善例:** 同じデータセットでも、目的変数のエンジニアリングや特徴量の変更を行うことで、回収率を大幅に向上させることが可能（例：123%）。

## Sources
- [youtube_I1eSN6mPANs (2713s–2785s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=2713)


## Related
- [[競馬モデルの精度評価：的中率と回収率のシミュレーション]]
- [[回収率の計算とモデル評価]]

