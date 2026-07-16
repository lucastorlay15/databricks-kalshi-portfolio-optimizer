import mlflow
import mlflow.sklearn

from mlflow.models import infer_signature

from src.modeling.model_config import (
    REGISTERED_MODEL_NAME,
)


def register_model(
    model,
    input_example,
    metrics: dict[str, float],
    params: dict,
    metadata: dict,
    decision_thresholds: dict,
    model_selection_artifact: dict,
    run_name: str,
) -> tuple[str, str]:
    mlflow.set_registry_uri(
        "databricks-uc"
    )

    signature = infer_signature(
        model_input=input_example,
        model_output=model.predict(
            input_example
        ),
    )

    with mlflow.start_run(
        run_name=run_name
    ) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        mlflow.log_dict(
            metadata,
            "model_metadata.json",
        )

        mlflow.log_dict(
            decision_thresholds,
            "decision_thresholds.json",
        )

        mlflow.log_dict(
            model_selection_artifact,
            "model_selection.json",
        )

        logged_model = (
            mlflow.sklearn.log_model(
                sk_model=model,
                name="model",
                signature=signature,
                input_example=input_example,
            )
        )

        registered_version = (
            mlflow.register_model(
                model_uri=(
                    logged_model.model_uri
                ),
                name=(
                    REGISTERED_MODEL_NAME
                ),
            )
        )

        run_id = run.info.run_id

    return (
        str(registered_version.version),
        run_id,
    )


def load_registered_model(
    model_version: str,
):
    mlflow.set_registry_uri(
        "databricks-uc"
    )

    model_uri = (
        f"models:/{REGISTERED_MODEL_NAME}/"
        f"{model_version}"
    )

    model = mlflow.sklearn.load_model(
        model_uri
    )

    return model, model_uri