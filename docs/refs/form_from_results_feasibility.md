# 馬 form 特徴を「results 自己結合」で再構成できるか — feasibility 調査

目的: 約2万件の馬ページ逐次取得（BAN の主因・netkeiba 依存）を、既存の results 表（＋race_info）
からの再構成で置き換え／削減できるか。`_horse_features.py` の各特徴を列可用性で仕分けた。

## 列の可用性

**results（`_results_processor`）** [index=race_id]: 着順, 枠番, 馬番, 斤量, 単勝, horse_id,
jockey_id, trainer_id, owner_id, 性, 年齢, 体重, 体重変化, n_horses(頭数), rank, rank_win。
**race_info マージで付与（race レベル）**: date, course_len, race_type, 馬場(ground_state1/2),
開催, race_class。

**results に無い＝馬ページ固有（`_horse_results_processor`）**: 通過(CORNER)→first/final_corner,
上り(NOBORI), ペース(PACE), タイム(TIME)→time_seconds→**speed_figure**, 着差(RANK_DIFF),
レース名(RACE_NAME), 天気(WEATHER), オッズ/人気, 馬体重, **賞金(PRIZE)**。

## 仕分け表（`_horse_features.py` の特徴ファミリ）

| 特徴ファミリ（関数） | 必要列 | results 自己結合で作れるか |
|---|---|---|
| recent_form（win/rentai/place/相対着順 × 5/9/20R） | 着順,頭数 | ✅ 可 |
| growth_trend / n_starts | 着順,頭数 | ✅ 可 |
| career_starts / wins / winrate | 着順 | ✅ 可 |
| career_earnings(_log) | **賞金** | ❌ 要ページ（or 払戻から） |
| prev_race（距離延長/斤量差/乗替） | course_len,斤量,jockey_id | ✅ 可（jockey_id で乗替判定） |
| aptitude（道悪勝率・競馬場別） | 着順,頭数,馬場,開催 | ✅ 可 |
| course_condition（距離帯/種別別） | 着順,頭数,course_len,race_type | ✅ 可 |
| type_ground（種別×馬場） | 着順,頭数,race_type,馬場 | ✅ 可 |
| race_class（同格/格上/最高勝利クラス） | 着順,頭数,race_class | ✅ 可（※微修正） |
| opponent_strength（faced grade） | レース名→grade | ✅ 可（race_class 代用に微修正） |
| **pace / leg_type（脚質）** | **通過(first_corner)** | ❌ 要ページ |
| **speed_figure（best/mean5）** | **タイム→speed_figure** | ❌ 要ページ |

※ race_class/opponent は現在「過去走のレース名を classify」するが、再構成フレームは race_class を
直接持てるので、その2関数を「レース名 parse → race_class 直接参照」に小改修すれば可。

## 結論

- **率・適性・クラス・距離系の form（高価値な大半）は results 自己結合で再構成可能＝スクレイピング不要**。
- **馬ページ必須は 3つだけ**: `speed_figure`（タイム由来）, `pace/脚質`（通過順由来）, `career_earnings`（賞金）。

## 実装が容易な理由（既存ガードと整合）

`_horse_features` は既に「列が無ければその特徴をスキップ」する設計:
- `add_pace_stats`: `if "first_corner" not in horse_results.columns: return results`
- `add_speed_figure_stats`: `if "speed_figure" not in horse_results.columns: return results`

→ **results から再構成した horse_results（通過/タイム列なし）を渡すと、pace/speed 系は自動スキップ、
率・適性・クラス・距離系は計算される**。既存の `_merge_horse_results`（date<target を searchsorted で
スライス→_add_* 適用）のパイプラインをそのまま流用でき、ロジック改変は最小。

## 設計: results→horse_results アダプタ

`build_horse_results_from_results(results_with_raceinfo) -> DataFrame[index=horse_id]`:
- results（race_info マージ済み）を horse_id index にし、HorseResultsCols 名へリネーム:
  着順←着順, 頭数←n_horses, 馬場←ground_state, 開催←開催, course_len, race_type, race_class,
  斤量, date, 騎手←jockey_id（乗替判定用の代理）。
- 通過/タイム/賞金は付与しない（→ pace/speed/earnings は自動スキップ）。
- `_data_merger._merge_horse_results` に「ページ由来 horse_results が空/欠損の horse_id は
  再構成フレームで補完」する分岐を追加（env `KEIBA_FORM_FROM_RESULTS=1` で有効化）。

## カバレッジへの効果（重要）

現在 form が 93–96% 欠損なのは horse_results.pkl が 824頭しか無いから。results は約23,000頭・
164,723行をカバー → **再構成すれば率系 form が全頭（取込み期間内）で即座に埋まる**（スクレイピング0件）。
制約: results はキャリアのうち**取込み期間内のみ**。期間前の走は欠ける（馬ページはキャリア全体）。
それでも「率系 form を 4% → ほぼ全頭」に引き上げる効果は大きい。

## 残ギャップの埋め方
- `speed_figure` / `pace(脚質)`: 過去分は Kaggle JRA データセット（Lap Times / Corner Passing Order,
  ～2021）で補完可。直近は必要な馬だけレース詳細ページを低頻度取得。
- `career_earnings`: 払戻(return)テーブル or race_info の賞金列があれば results 側で補完可（要確認）。

## 推奨
1. アダプタ `build_horse_results_from_results` を純関数で実装＋単体テスト（リーク無しは既存の
   date スライスに委譲）。
2. `_merge_horse_results` に補完分岐（env フラグ）を追加。
3. `rebuild-featured`（scraping 0）→ 非null率の激増を確認 → `debug_leak.py` → `stack_eval.py --years 2026`
   で baseline と比較。speed/pace は後追いで補完。
