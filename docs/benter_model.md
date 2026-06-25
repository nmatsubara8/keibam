# ベンター・モデル — プロジェクト適用のための知識整理

出典: William Benter (1994), *"Computer Based Horse Race Handicapping and Wagering
Systems: A Report"*, in *Efficiency of Racetrack Betting Markets* (Hausch, Lo, Ziemba 編).

香港競馬で実運用し長期利益を上げた手法の公開報告。本プロジェクトの設計（2ヘッド勝率モデル
＋確定オッズ EV ＋ケリー）の原典。**特に三連単/三連複の確率計算**で使うエッセンスを以下に整理。

---

## 1. 全体アーキテクチャ（2段モデル）

1. **ファンダメンタルズ・モデル**（条件付きロジット）で各馬の勝率 `P_fund(i)` を出す。
2. **公衆の implied 勝率** `P_public(i)`（オッズ由来）と**2段目ロジットで合成**:
   $$\pi_i = \frac{\exp(\alpha f_i + \beta h_i)}{\sum_j \exp(\alpha f_j + \beta h_j)},\quad f=\log P_\text{fund}^{(OOS)},\ h=\log P_\text{public}$$
   `(α,β)` は勝ち馬ラベルの最尤推定。**`f` は out-of-sample のファンダ予測**を使う（重要）。
3. 合成勝率 `π` から **Harville（補正版）**で連系の的中確率を出し、`EV = π·O_final − 1` で選定。
4. **分数ケリー**で資金配分。

### 核心の教訓（Tipster の例）
- 単独 R² が公衆並みでも、**合成後の ΔR²（= R²_combined − R²_public）がほぼ 0 なら「市場の写し」で無価値**。
- 価値は「**市場に上乗せした独立情報**」でのみ測る → 指標は **ΔR²(combined vs public)**。
- → 本プロジェクトの `--no-odds-features` A/B・`edge_diagnostic` の echo/logloss はこれの近似。
  **ΔR² の実装が望ましい**（より論文準拠で解釈明快）。

---

## 2. 三連単・三連複の計算（実装の主役）

### 素の Harville はバイアスがあり、補正なしで使ってはいけない
Benter は明言: *"This formula is significantly biased, and should not be used for betting
purposes."*（Harville 1973 の素の式は2着・3着の確率を系統的に誤る。人気馬の複勝/連対を過大評価）。

### べき乗補正（Henery 1981 / Stern 1990 / Lo & Bacon-Shone 1992）— 実装済み
勝率 `π` から**着位ごとに尖り方を変えた配列**を作る:

$$\sigma_i = \frac{\pi_i^{\gamma}}{\sum_j \pi_j^{\gamma}}\ (\text{2着用}),\qquad
  \tau_i = \frac{\pi_i^{\delta}}{\sum_j \pi_j^{\delta}}\ (\text{3着用})$$

**三連単**（i→j→k）:
$$P(i\to j\to k) = \pi_i \cdot \frac{\sigma_j}{1-\sigma_i} \cdot \frac{\tau_k}{1-\tau_i-\tau_j}$$

**三連複**（{i,j,k}）= 全6順列の三連単の和。

- `γ,δ` は**競馬場ごとに異なる**（普遍定数ではない）。Benter の香港データでは **γ≈0.81, δ≈0.65**
  （1未満＝人気馬の2/3着を抑える方向）。**自前データで最尤推定すべき**。
- `γ=δ=1` で素の Harville に一致（後方互換）。補正後も全順列の確率は 1 に正規化される。

### 本プロジェクトでの実装（`src/policies/_harville.py`）
| 関数 | 役割 |
|---|---|
| `PlaceExponents(gamma, delta)` | 着位別べき指数 DTO。既定 (1,1)=素の Harville。`PlaceExponents.BENTER_HK`=(.81,.65) |
| `place_adjusted(win_probs, exp)` | `π^exp / Σπ^exp`（σ/τ 配列・式7/8） |
| `prob_trifecta_corrected(wp, f, s, t, exp)` | 補正三連単（式9） |
| `prob_trio_corrected(wp, a, b, c, exp)` | 補正三連複（6順列の和） |
| `prob_exacta_corrected` / `prob_quinella_corrected` | 馬単/馬連の補正版（2着に σ） |
| `fit_place_exponents(races, init=(.81,.65))` | 過去レースの 1-2-3着から (γ,δ) を MLE（OOS 勝率で） |
| `combo_probability(bt, wp, combo, exponents=…)` | `exponents` 指定で順序券種（馬単/馬連/三連単/三連複）に補正適用 |

複勝・ワイドは **Place ヘッド直接**（§3-34）が正路なので補正対象外（win-Harville 補正は順序券種のみ）。

---

## 3. ウェイジャー戦略

- 優位: `er = c · div`、`advantage = er − 1`（c=合成勝率, div=配当）。**正の優位の券種だけ買う**。
- **最低優位しきい値（≥10%）**を課す（優位の過大評価対策。Ziemba & Hausch 1987）。
  → 本プロジェクトの `ev>1.1`（§3-41）と整合。
- **確率を先に補正 → その後に優位計算**（順序厳守）。
- **分数ケリー**（1/2〜1/3）。単勝: `K = advantage / (div − 1)`。
  → 本プロジェクトの kelly×0.25 と整合（むしろ保守的で良い）。
- **券種が specific なほど優位が高い**（exotic=三連単等は公衆の推定が薄まり穴のエッジが残る）。
- **unratable 馬（初出走・データ無し）→ 公衆確率をそのまま割当**。初出走のみのレースは除外（~5%）。

---

## 4. プール影響と現実（実運用の制約）

- **自分の賭けが配当を下げる**（パリミュチュエル）。資金がプールに対し大きいほど支配的。
  最適ベットは驚くほど小さい（**1レース turnover の 0.25〜0.5%**、小規模なら 0.1〜0.2%）。
- **小売上のレース場は採算外**（控除が高く、賭けられる量が小さく、直前オッズが不安定）。
- Benter の実績: **DB+モデルに ~5人年、高収益化にさらに ~5人年。控除 ~19%、年 ~470 レース。
  5シーズン中4黒字、負け年は資本の ~20% DD。最初の ~700 ベットは横ばい**。
  → **インサンプル backtest の派手な数字は非現実的**。エッジは薄く、大量試行と長期で実現する。

---

## 5. リーク/過学習の戒め（本プロジェクトの宿題）

- **out-of-sample 検証必須**（data partitioning）。OOS 精度は条件付きロジットでは ~1000 レースで頭打ち。
- 合成の `f`（ファンダ確率）は **OOS 予測**でなければならない（時系列 holdout 予測の生成器が要る）。
- 「number of past races」のような直感に反する有意因子は**大標本でのみ確立**。

---

## 6. 実装キュー（本ドキュメント由来の TODO）

- [x] **三連単/三連複のべき乗補正 Harville**（`_harville.py`・テスト済み）
- [ ] **(γ,δ) を自前データで MLE**（`fit_place_exponents` を OOS 着順で実行 → `models/place_exponents.json`）
- [ ] EV パイプライン（`ExpectedValueBetPolicy` / `_backtest`）へ `exponents` を配線（opt-in）
- [ ] **ΔR²（combined vs public）診断**を `_edge_diagnostic` に追加
- [ ] **合成モデル**（`combine_logpool` + `fit_blend`）— `P_fund × P_public` の2段目ロジット
- [ ] **unratable → 公衆フォールバック**
- [ ] （実運用）プール影響・賭けサイズ上限（0.1〜0.5%/レース）

---

## 参考文献（p.198 より主要なもの）
- Harville (1973) — 素の着順確率式
- Henery (1981), Stern (1990) — 順列分布モデル（べき乗補正の理論）
- Lo & Bacon-Shone (1992); Lo, Bacon-Shone & Busche (1994) — 補正の簡便法・ランキング確率
- Bolton & Chapman (1986) — 多項ロジットによる handicapping
- Asch & Quandt (1986); White, Dattero, Flores (1992) — 合成（2段目ロジット）
- Kelly (1956); MacLean, Ziemba, Blazenko (1992); Kallberg & Ziemba (1994) — ケリー/資金配分
- Ali (1977) — 人気-穴バイアス / Ziemba & Hausch (1986,1987) — place/show 戦略
