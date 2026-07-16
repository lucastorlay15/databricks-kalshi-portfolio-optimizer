from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.modeling.model_config import (
    HIST_GRADIENT_BOOSTING_PARAMETERS,
    RIDGE_PARAMETERS,
)


def build_model(model_name: str) -> Pipeline:
    if model_name == "dummy_mean":
        return Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="median"),
                ),
                (
                    "model",
                    DummyRegressor(strategy="mean"),
                ),
            ]
        )

    if model_name == "ridge":
        return Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="median"),
                ),
                (
                    "scaler",
                    StandardScaler(),
                ),
                (
                    "model",
                    Ridge(**RIDGE_PARAMETERS),
                ),
            ]
        )

    if model_name == "hist_gradient_boosting":
        return Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="median"),
                ),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        **HIST_GRADIENT_BOOSTING_PARAMETERS
                    ),
                ),
            ]
        )

    raise ValueError(
        f"Unsupported model name: {model_name!r}"
    )


def build_candidate_models() -> dict[str, Pipeline]:
    return {
        model_name: build_model(model_name)
        for model_name in [
            "dummy_mean",
            "ridge",
            "hist_gradient_boosting",
        ]
    }


def train_model(
    model_name: str,
    X_train,
    y_train,
) -> Pipeline:
    model = build_model(model_name)

    model.fit(
        X_train,
        y_train,
    )

    return model