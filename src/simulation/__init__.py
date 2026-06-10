from src.simulation._metrics import (
    classification_metrics,
    max_drawdown,
    summarize_returns,
)
from src.simulation._plot import (
    plot_calibration,
    plot_ev_threshold_sweep,
    plot_ev_weight_curve,
    plot_odds_prediction_accuracy,
    plot_single_threshold,
    plot_stacking_contribution,
)
from src.simulation._popularity_baseline import PopularityBaselineSimulator
from src.simulation._simulator import Simulator

__all__ = [
    "Simulator",
    "PopularityBaselineSimulator",
    "plot_single_threshold",
    "plot_calibration",
    "plot_ev_threshold_sweep",
    "plot_odds_prediction_accuracy",
    "plot_stacking_contribution",
    "plot_ev_weight_curve",
    "max_drawdown",
    "summarize_returns",
    "classification_metrics",
]
