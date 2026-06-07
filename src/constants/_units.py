"""数値スケール・単位の定数（マジック数値の集約先）。

意味の異なる「100」が複数モジュールに散在していたため、ここに名前付きで集約する。
他 src レイヤへの依存は持たない（constants_purity 契約）。
"""

# JRA の払戻オッズは「100 円賭けに対する払戻金（円）」で表示される慣行のため、
# 任意の amount 円賭けの払戻金は `return_yen * amount / PAYOUT_UNIT_YEN`。
# 例: return_yen=350 で 100 円賭け → 350 円戻り（倍率 3.5）。
PAYOUT_UNIT_YEN = 100

# コース距離（メートル）を 100m バケットに丸めて特徴量化する際のスケール。
# 例: course_len=1600m → 16（バケット）。
COURSE_LEN_BUCKET_METERS = 100
