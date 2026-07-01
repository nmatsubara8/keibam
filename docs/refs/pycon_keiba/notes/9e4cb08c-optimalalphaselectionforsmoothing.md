---
title: "Optimal Alpha Selection for Smoothing"
category: workflow
confidence: 0.90
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0030
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1856s – 1899s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1856"
---

When implementing smoothing, the parameter $alpha$ cannot be chosen arbitrarily. It is necessary to select an optimal value for $alpha$ for every category.

**Method:** This selection process should consider the relationship between the feature and the target variable, such as calculating the mutual information (相互情報量) with the target variable.

**Example:** The video demonstrates that even if Category A appears to have a higher rate (50% vs 40%), a calculation using a specific $alpha$ (e.g., 0.1 with a count of 100) might show that Category B is actually stronger, providing a more intuitive result.

## Sources
- [youtube_I1eSN6mPANs (1856s–1899s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1856)


## Related
- [[Smoothing Ratio Features (スムージング)]]

