"""RL / マルコフ(HMM)手法の競馬無効性の実証（データ不要・純シミュレーション）。

## 何を示すか

効率的パリミュチュエル市場では、強化学習も隠れマルコフによるレジーム検出も市場を破れない。
状態に無い情報は作り出せないからで、最適方策は **abstain（賭けない）** に収束する。
これを対照実験で示す:

  効率市場（= 公開 JRA 単勝, echo≈0.989）:
    - 素朴ベット           → 控除率ぶん負ける
    - RL(粗い文脈)         → ほぼ全レース「賭けない」に収束（勝てない）
    - RL(細かい偽文脈)     → in-sample だけ利益、OOS で消滅（＝動画300%と同じ過学習）
    - HMM レジーム戦略     → レジームは価格済みで無価値（負ける）

  stale 市場（= 非効率, Benter の香港プール相当。対照群）:
    - 同じ RL / 同じ HMM 戦略が**勝つ**

同一アルゴリズムが市場効率の有無だけで勝敗が反転する ⇒ 無効性の原因はアルゴリズムでは
なく**市場効率**。公開データが効率的である以上、手法をいくら高度化しても edge は出ない。

使い方:
    python verify_rl_markov_futility.py                    # 既定 seed=0
    python verify_rl_markov_futility.py --seed 7 --n 3000
"""

from __future__ import annotations

import argparse
import sys

from src.simulation._efficient_market import MarketConfig, run_futility_experiment


def _rule(title: str) -> None:
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def _fmt_pct(x: float) -> str:
    return f"{x * 100:+6.1f}%"


def _report_mode(name: str, m: dict, takeout: float) -> None:
    base = m["baseline"]
    coarse = m["bandit"]
    over = m["overfit_bandit"]
    reg = m["regime"]
    print(f"\n[{name}]  控除率 = {takeout:.1%}")
    print(f"  素朴ベット(本命)         : 平均純損益 {_fmt_pct(base['mean_net'])}  "
          f"回収率 {base['recovery_rate']:.3f}  的中 {base['hit_rate']:.3f}")
    print(f"  RL 粗い文脈  OOS         : 平均純損益 {_fmt_pct(coarse['oos']['mean_net'])}  "
          f"賭けない率 {coarse['oos']['abstain_fraction']:.1%}  賭けた数 {coarse['oos']['n_bets']}")
    print(f"  RL 細かい偽文脈 in-sample: 賭け当り純損益 {_fmt_pct(over['in_sample']['mean_net_per_bet'])}  "
          f"賭けた数 {over['in_sample']['n_bets']}")
    print(f"  RL 細かい偽文脈 OOS      : 賭け当り純損益 {_fmt_pct(over['oos']['mean_net_per_bet'])}  "
          f"賭けた数 {over['oos']['n_bets']}")
    print(f"  HMM レジーム戦略 OOS     : 賭け当り純損益 {_fmt_pct(reg['mean_net_per_bet'])}  "
          f"賭けた数 {reg['n_bets']}  出動率 {reg['coverage']:.1%}")


def main() -> int:
    ap = argparse.ArgumentParser(description="RL/マルコフ手法の競馬無効性の実証")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--n", type=int, default=1500, help="train/test 各レース数")
    ap.add_argument("--takeout", type=float, default=0.20)
    ap.add_argument("--overfit-bins", type=int, default=40, help="偽文脈の分割数（過学習の強さ）")
    args = ap.parse_args()

    cfg = MarketConfig(takeout=args.takeout)
    r = run_futility_experiment(
        n_train=args.n,
        n_test=args.n,
        config=cfg,
        seed=args.seed,
        overfit_noise_bins=args.overfit_bins,
    )
    takeout = r["takeout"]

    _rule("RL / マルコフ手法の競馬無効性 — 効率市場 vs 非効率市場の対照実験")
    print(f"設定: 頭数 {cfg.n_horses}, 控除率 {takeout:.1%}, レジーム持続 {cfg.p_stay}, "
          f"seed {args.seed}, train/test 各 {args.n} レース")

    _report_mode("効率市場（= 公開 JRA 単勝, echo≈0.989）", r["efficient"], takeout)
    _report_mode("stale 市場（= 非効率, 対照群. Benter の香港プール相当）", r["stale"], takeout)

    eff = r["efficient"]
    stale = r["stale"]

    _rule("読み取り")
    print("① 効率市場では『賭けない』が最適:")
    print(f"   RL(粗い文脈)は OOS で {eff['bandit']['oos']['abstain_fraction']:.0%} のレースを見送り、"
          f"平均純損益 {_fmt_pct(eff['bandit']['oos']['mean_net'])}（利益ゼロ）。")
    print("   状態に無い情報は作れない。最適方策は abstain に収束する。")
    print()
    print("② 『300%』は文脈の切り過ぎ（過学習）で作れるが OOS で消える:")
    over_is = _fmt_pct(eff["overfit_bandit"]["in_sample"]["mean_net_per_bet"])
    over_oos = _fmt_pct(eff["overfit_bandit"]["oos"]["mean_net_per_bet"])
    print(f"   偽文脈を{args.overfit_bins}分割 → in-sample 賭け当り {over_is} だが OOS {over_oos}。")
    print("   動画の回収率300%と同じ機序（test 上での買い目閾値の後付け最適化）。")
    print()
    print("③ 無効性の原因はアルゴリズムでなく市場効率（対照群）:")
    print(f"   同じ HMM レジーム戦略が、効率市場 {_fmt_pct(eff['regime']['mean_net_per_bet'])} → "
          f"stale 市場 {_fmt_pct(stale['regime']['mean_net_per_bet'])} と反転して勝つ。")
    print(f"   同じ RL も、効率市場 {_fmt_pct(eff['bandit']['oos']['mean_net'])} → "
          f"stale 市場 {_fmt_pct(stale['bandit']['oos']['mean_net'])}。")
    print("   edge は『市場がまだ価格に織り込んでいない情報』からのみ生まれる。公開 JRA データは")
    print("   効率的（echo≈0.989, ΔR²≈0）でその余地が無い。Markov/深層RL の高度化では覆らない。")

    _rule("結論")
    print("  ・効率的パリミュチュエルでは RL/HMM は市場を破れない。最適解は abstain（賭けない）。")
    print("  ・見かけの高回収率は文脈/閾値の後付け最適化（in-sample 過学習）で、OOS で必ず消える。")
    print("  ・手法ではなく市場効率が壁。公開データで edge を作る道は閉じている（→ 損失最小化へ）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
