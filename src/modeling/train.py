from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
import numpy as np

from src.modeling.model_config import (
    RIDGE_PARAMETERS,
    HIST_GRADIENT_BOOSTING_PARAMETERS,
)


def build_model(model_name):

    if model_name == "ridge":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", Ridge(**RIDGE_PARAMETERS)),
            ]
        )

    if model_name == "hist_gradient_boosting":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        **HIST_GRADIENT_BOOSTING_PARAMETERS
                    ),
                ),
            ]
        )

    raise ValueError(model_name)


def train_model(
    model_name,
    X_train,
    y_train,
):
    model = build_model(model_name)

    model.fit(
        X_train,
        y_train,
    )

    return model


def evaluate_model(
    model,
    X_test,
    y_test,
):
    predictions = model.predict(X_test)

    metrics = {
        "test_mae": mean_absolute_error(
            y_test,
            predictions,
        ),
        "test_rmse": np.sqrt(
            mean_squared_error(
                y_test,
                predictions,
            )
        ),
        "test_r2": r2_score(
            y_test,
            predictions,
        ),
    }

    return (
        predictions,
        metrics,
    )