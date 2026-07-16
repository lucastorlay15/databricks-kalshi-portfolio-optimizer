import numpy as np
import pandas as pd

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)


def calculate_regression_metrics(
    actual: pd.Series | np.ndarray,
    predicted: pd.Series | np.ndarray,
) -> dict[str, float]:
    actual_array = np.asarray(actual)
    predicted_array = np.asarray(predicted)

    nonzero_actual_mask = actual_array != 0

    if nonzero_actual_mask.any():
        directional_accuracy_nonzero = np.mean(
            np.sign(
                predicted_array[
                    nonzero_actual_mask
                ]
            )
            == np.sign(
                actual_array[
                    nonzero_actual_mask
                ]
            )
        )
    else:
        directional_accuracy_nonzero = np.nan

    return {
        "mae": float(
            mean_absolute_error(
                actual_array,
                predicted_array,
            )
        ),
        "rmse": float(
            np.sqrt(
                mean_squared_error(
                    actual_array,
                    predicted_array,
                )
            )
        ),
        "r2": float(
            r2_score(
                actual_array,
                predicted_array,
            )
        ),
        "prediction_mean": float(
            predicted_array.mean()
        ),
        "prediction_stddev": float(
            predicted_array.std()
        ),
        "directional_accuracy_all": float(
            np.mean(
                np.sign(predicted_array)
                == np.sign(actual_array)
            )
        ),
        "directional_accuracy_nonzero": float(
            directional_accuracy_nonzero
        ),
    }


def evaluate_model(
    model,
    X,
    y,
) -> tuple[np.ndarray, dict[str, float]]:
    predictions = model.predict(X)

    metrics = calculate_regression_metrics(
        actual=y,
        predicted=predictions,
    )

    return predictions, metrics


def calculate_opportunity_thresholds(
    predictions: pd.Series | np.ndarray,
    percentiles: list[float],
) -> dict[float, float]:
    prediction_series = pd.Series(
        predictions
    )

    return {
        percentile: float(
            prediction_series.quantile(
                percentile
            )
        )
        for percentile in percentiles
    }