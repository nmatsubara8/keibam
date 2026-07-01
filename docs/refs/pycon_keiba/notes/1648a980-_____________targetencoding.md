---
title: "ターゲットエンコーディング (Target Encoding)"
category: concept
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0025
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1623s – 1678s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1623"
---

ターゲットエンコーディングとは、過去のデータから特定のカテゴリーに関連する目的変数を集計し、それを特徴量として利用する手法です。

**手順の概要:**
1. **集計:** 特定のカテゴリー（例：父馬）に属するデータ群について、目的変数（例：着順、勝率）を統計的に集計します。
2. **特徴量化:** この集計値（例：同父馬の平均着順）を、そのカテゴリーの新しい特徴量としてモデルに入力します。

この考え方は、目的変数に限らず、的中率や回収率など様々な数値指標で集計することが可能です。

**応用:**
*   **自動化:** この考え方を一般化することで、大量の特徴量を機械的に作成できます。
*   **特徴量生成の構造:** 「誰が」「どのようなレース条件で」「どのような統計値になったか」という3つの組み合わせで集計特徴量を表現できます。


## Sources
- [youtube_I1eSN6mPANs (1623s–1678s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1623)


## Related
- [[ターゲットエンコーディングの概念]]

