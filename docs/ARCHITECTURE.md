# アーキテクチャ（保守性・疎結合）

特定ロジックの修正影響が追跡不能になる設計を禁止するため、以下の原則を全モジュールに適用する。

## レイヤ単方向依存

下位 → 上位の順（上位は下位のみ import 可）:

```
constants  →  preprocessing / preparing  →  policies  →  training  →  portfolio  →  simulation  →  operation  →  pipeline  →  ui
```

- `policies`（馬券/スコア戦略・Harville・オッズ供給）は `constants` のみに依存する低位の戦略プリミティブ。
- `training` の `KeibaAI` がモデルと policies を束ねるオーケストレータのため `policies < training`。
- 逆方向（上位への）import は **`tests/architecture/test_layering.py`** が AST 解析で検出し失敗させる。
- 依存関係グラフは `import-linter`（`.importlinter`）でも契約化（CI で全依存インストール時に実行）。

## 境界（抽象インターフェース）と依存性注入

実装差し替えの影響を一点に閉じ込めるため、境界は `abc.ABC` で定義し、実装はコンストラクタ注入する。

| 抽象 | 役割 | 実装例 |
|---|---|---|
| `AbstractOddsProvider` | オッズ供給 | `HistoricalOddsProvider`（過去推定）/ ライブ（将来） |
| `AbstractOddsPredictor` | 締切確定オッズ予測(Layer2) | `IdentityOddsPredictor` / `LgbOddsPredictor` |
| `AbstractConfidenceScorer` | 確信度 | `CompositeConfidenceScorer` |
| `AbstractPortfolioOptimizer` | 資金配分 | `KellyPortfolioOptimizer` |
| `AbstractBetExecutor` | 馬券実行 | `Advisory` / `SemiAuto` / `Auto`（運用モード） |
| `StackingModel` の base/meta | 勝率モデル | LightGBM / `NnWinModel`（DI） |

## 不変 DTO と副作用の隔離

- レイヤ間の受け渡しは `dataclass(frozen=True)` の DTO（例 `BetCandidate`）。途中改変による波及を防ぐ。
- I/O（スクレイピング・ファイル永続化・UI・発注）はアダプタ層（`preparing`/`operation`/`ui`）に隔離し、
  ドメインロジック（`policies`/`portfolio`/`simulation/_metrics`/`training` の学習器）は純粋に保つ。
- マジックナンバー/文字列は `constants` に一元化（`BetType`/`BetThresholds`/`RiskLimits`）。

## 重い依存の隔離

`torch`（`NnWinModel`）・`optuna`（`ModelWrapper`）等は遅延 import し、未導入環境でも他モジュールの
import を壊さない。これにより純粋ドメイン層は軽量依存だけでテスト可能。

## ADR（主要判断）

- **Layer1=GBDT×DL スタッキング**: 数値分岐(GBDT)と非線形(NN)の得意分野を meta 学習器で結合。
- **Layer2=段階拡張のオッズ予測**: ①LightGBM回帰+レース内正規化 →②分位点 →③時系列DL。
- **過去の連オッズは遡及不可**: バックテストは Harville 推定オッズ、ライブは実オッズ。
- **較正は test クリーンを厳守**: base→meta→較正ホールドアウトの時系列分割。
- **運用は当面 advisory**: `full_auto` は設定で将来有効化（既定無効、規約・法的リスク）。
