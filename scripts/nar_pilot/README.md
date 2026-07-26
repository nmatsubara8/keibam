# NAR（地方競馬）市場効率性 パイロット

「特定の地方競馬は流動性が薄く非効率が残るのでは？」を netkeiba の実データで検証する
自己完結パイロット。結論・数値は `docs/nar_efficiency_pilot.md` に記録。

## パイプライン

```
python scripts/nar_pilot/scrape_win.py      --data-dir data/nar_pilot   # 単勝/着順を取得
python scripts/nar_pilot/scrape_exotic.py   --data-dir data/nar_pilot   # 連系払戻を取得
python scripts/nar_pilot/diagnose_win.py    --data-dir data/nar_pilot   # 単勝FLB/回収率
python scripts/nar_pilot/diagnose_exotic.py --data-dir data/nar_pilot   # 連系Harville回収率+echo
python scripts/nar_pilot/diagnose_models.py --data-dir data/nar_pilot   # 素/割引Harville/市場のLL比較
```

`--data-dir`（既定 `data/nar_pilot`, git 管理外）に CSV を書き出す。スクレイパは対象場
（既定 大井44/高知54/佐賀55）を礼儀正しく取得（curl 経由・1req/1秒・指数バックオフ・場別上限）。

## 結論（2026-05〜06, 475レース）

- **単勝**: FLB（本命過小評価）は実在し JRA より強いが、本命回収率 0.86（佐賀0.91）< 控除20% ＝赤字。
- **連系**: 三連単プールは単勝Harvilleを echo 0.94 でなぞる。本命連系は全戦略・全場で回収率<1.0。
- **モデル比較**: 割引Harville は素Harvilleより改善するが、**市場含意が全場で最良予測器**。
  天井は単勝そのもの。単勝と直交する情報だけが残る扉だが JRA echo 0.989/ΔR²≈0 が見込みの薄さを示す。
- **総括**: 地方は測定上たしかに非効率だが「非効率 < 控除率」が全プールで成立＝公開データで黒字化せず。

## 注意

- 取得データは第三者(netkeiba)由来・ephemeral のため **repo にはコミットしない**（`data/` は gitignore）。
  数値は `docs/nar_efficiency_pilot.md` に保存。再現は上記スクレイパで再取得する。
- 標本は2ヶ月・3場（方向性の確認）。ファンダメンタルモデルは未使用（単勝＋払戻のみ）。
