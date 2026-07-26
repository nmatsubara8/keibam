---
title: "One-Hot Encodingの仕組み"
category: algorithm
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - raw_s0310
  - raw_s0311
  - raw_s0312
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "1605s – 1614s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=1605"
---

カテゴリカルデータに対して最も一般的に行われる数値化手法の一つです。

1. **目的**: カテゴリカルな特徴量を機械学習モデルが扱える数値形式に変換すること。
2. **手順**: カテゴリカルな特徴量の種類（カテゴリ数）分だけのバイナリベクトルを用意します。
3. **適用**: その特徴量が持つ値に対応する次元のみを `1` に設定し、その他の次元は `0` に設定します。

**例**: カテゴリがA, B, Cの3種類の場合、[A, B, C] の3次元ベクトルを用意し、Aの場合は `[1, 0, 0]`、Bの場合は `[0, 1, 0]`、Cの場合は `[0, 0, 1]` となります。

## Sources
- [youtube_I1eSN6mPANs (1605s–1614s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=1605)


## Related
- [[One-Hot Encoding and Target Variable Transformation]]

