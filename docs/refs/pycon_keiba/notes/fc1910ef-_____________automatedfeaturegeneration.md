---
title: "自動特徴量生成と組み合わせ (Automated Feature Generation)"
category: workflow
confidence: 0.90
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0026
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1688s – 1739s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1688"
---

ターゲットエンコーディングの考え方を一般化し、大量の特徴量を機械的に作成するワークフローです。

**特徴量生成の軸:**
*   **エンティティ:** 馬、騎手、調教師、馬主、父馬など。
*   **コンテキスト:** トラックタイプ、コース、距離、天候、馬場状態、ペースなど。
*   **集計軸:** 上記のエンティティとコンテキストの組み合わせに基づき、統計値（例：平均着順、勝率）を計算します。

この組み合わせにより、1500以上の特徴量を自動生成し、予測モデルに組み込むことが可能です。

**戦略的利用:**
*   良質な特徴量を積極的に追加することがモデル性能の飛躍的な向上に繋がります。
*   「とりあえず力で殴れるところは殴っておく」という戦略的なアプローチも有効です。


## Sources
- [youtube_I1eSN6mPANs (1688s–1739s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1688)


## Related
- [[ターゲットエンコーディング (Target Encoding)]]
- [[競馬データの特徴量エンジニアリング：コンテキスト別集計]]
- [[特徴量エンジニアリングの集計処理]]

