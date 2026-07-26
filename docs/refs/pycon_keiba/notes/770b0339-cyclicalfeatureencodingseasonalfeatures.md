---
title: "Cyclical Feature Encoding (Seasonal Features)"
category: concept
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0031
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1915s – 1972s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1915"
---

For features that exhibit periodicity (like time/seasonality), simply treating months (1-12) as standard categorical variables is suboptimal. A more advanced method is to encode these cyclical features using sine and cosine transformations.

**Method:** Map the cyclical feature (e.g., month 1 to 12) onto a 2D plane using $sin(	heta)$ and $cos(	heta)$ representations.

**Benefit:** This preserves the cyclical nature of the data (e.g., the distance between December and January is small, unlike the distance between January and December if treated linearly). This technique is recommended for any feature exhibiting periodicity.

**Example Context:** In horse racing data, performance might change significantly between seasons (e.g., stronger in hot weather, weaker during estrus cycles in spring).

## Sources
- [youtube_I1eSN6mPANs (1915s–1972s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1915)


## Related
- [[周期的な特徴量のエンコーディング (Cyclical Feature Encoding)]]

