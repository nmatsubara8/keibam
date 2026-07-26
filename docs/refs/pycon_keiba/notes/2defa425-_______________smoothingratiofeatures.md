---
title: "データスパース性への対応：平滑化 (Smoothing Ratio Features)"
category: concept
confidence: 0.90
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0028
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1755s – 1798s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1755"
---

集計特徴量を用いる際、特定のカテゴリのデータ数が極端に少ない（レアケース）と、統計値の信頼度（確信度）が著しく低下し、予測に大きな「ガタツキ」が生じる問題があります。

**対策（平滑化）:**
*   該当カテゴリのデータが少ない場合、集計値を**全体平均値**に近づける（補正する）必要があります。
*   この補正の度合いを決定するパラメータ $\alpha$ は、カテゴリごとに最適値を選択することが推奨されます。

**重要性:**
*   データが少ない場合の極端な値（例：2回走って1勝＝50%）と、十分なデータに基づく値（例：1000回走って500勝＝50%）では、統計的な信頼度に大きな差があるため、平滑化による補正が不可欠です。


## Sources
- [youtube_I1eSN6mPANs (1755s–1798s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1755)

