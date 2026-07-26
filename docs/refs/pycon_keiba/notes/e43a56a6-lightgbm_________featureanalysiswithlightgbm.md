---
title: "LightGBMを用いた特徴量分析（Feature Analysis with LightGBM）"
category: tool_X
confidence: 0.95
review_status: auto_accepted
last_verified: 2026-07-01
derived_from:
  - sem_0047
sources:
  - video_id: youtube_I1eSN6mPANs
    channel: PyCon JP
    timestamp: "2383s – 2464s"
    url: "https://www.youtube.com/watch?v=I1eSN6mPANs&t=2383"
---

LightGBMは、モデル学習に直接関係しないものの、特徴量の分析を行うのに非常に有用です。

**1. マクロな特徴量分析 (Macro-level Feature Analysis)**:
* どの特徴量がモデルの予測に影響を与えているか（特徴量の重要度）を確認する。

**2. ミクロな特徴量分析 (Micro-level Feature Analysis)**:
* 特定の入力データ（例：あるレース）に対する予測において、どの特徴量がどれだけ寄与したのか（予測の根拠）を詳細に分析する。

このミクロな分析は、予測の根拠を明確にし、信頼性を高めるために重要です。Shapley値などの手法も利用可能です。

*💡 **応用例**: 多数の特徴量（例：何千もの特徴量）を、関連性の高いグループ（例：6〜7個のファクター）に分類し、各グループの貢献度を合計して可視化することで、予測の全体的な根拠を示すチャートを作成できます。

## Sources
- [youtube_I1eSN6mPANs (2383s–2464s)](https://www.youtube.com/watch?v=I1eSN6mPANs&t=2383)


## Related
- [[モデルの重要度解析と特徴量の解釈]]

