---
title: "LightGBMチューニングにおけるカテゴリ変数の扱い"
category: concept
confidence: 0.90
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0043
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "2323s – 2337s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=2323"
---

LightGBMのチューニングに関するTipsとして、カテゴリ変数の扱いに注意が必要です。

*   ドキュメント上ではカテゴリ変数をそのまま扱うことが可能とされていますが、実際には**ダミー変数化（One-Hot Encodingなど）**を行った方が高い精度が出ることが多い傾向があります。
*   ただし、カテゴリ変数の数が非常に大きくなる場合は、精度が出にくい可能性も考慮する必要があります。

## Sources
- [youtube_I1eSN6mPANs (2323s–2337s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=2323)


## Related
- [[LightGBM カテゴリ変数の扱い方とラベルエンコーディング]]

