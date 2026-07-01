---
title: "Smoothing Ratio Features (スムージング)"
category: concept
confidence: 0.90
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0029
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1806s – 1849s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1806"
---

This technique is used as a post-processing step to achieve better intuition, especially when a category has few occurrences. 

**Goal:** To adjust the aggregate value of a category with few counts closer to the overall average.

**Formula Components (Conceptual):**
*   $R$: Aggregate value for the specific category.
*   $R_{average}$: The overall average.
*   $N$: The frequency/occurrence count of the category.
*   $alpha$: A weighting parameter.

The calculation involves a formula where the influence of the category's aggregate value is tempered by its frequency relative to the overall average. As $N$ becomes very large, the left side of the equation approaches 1, and if $N$ is small, the process pulls the value closer to $R_{average}$.

**Key Takeaway:** The parameter $alpha$ is crucial and often requires selecting an optimal value for *each* category, potentially by examining metrics like mutual information with the target variable.

## Sources
- [youtube_I1eSN6mPANs (1806s–1849s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1806)


## Related
- [[データスパース性への対応：平滑化 (Smoothing Ratio Features)]]

