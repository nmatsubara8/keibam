---
title: "複数の評価指標によるモデル比較の重要性"
category: concept
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0065
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "2621s – 2674s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=2621"
---

単一の評価指標（例：単なるNDCGスコア）だけではモデルの性能を完全に読み取ることができない場合がある。

*   **問題点**: あるモデルが特定の指標（例：着順のNDCG）で高いスコアを示しても、別の指標（例：単勝支持率のNDCG）では低いスコアを示すことがある。
*   **解決策**: 複数の異なる観点（例：着順、賞金、単勝支持率など）からNDCGを評価し、それらを総合的に比較することで、モデルの強みと弱みを多角的に把握できる。

**例**: AモデルとBモデルの比較において、Aモデルが着順や賞金関連のNDCGで優れている一方、Bモデルが単勝支持率のNDCGで優れているケースが示された。

## Sources
- [youtube_I1eSN6mPANs (2621s–2674s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=2621)


## Related
- [[ランキング問題の評価指標 nDCG]]

