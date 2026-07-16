import mlflow
import mlflow.sklearn

from mlflow.models import infer_signature

from src.modeling.model_config import (
    REGISTERED_MODEL_NAME,
)


def register_model(
    model,
    input_example,
    metrics,
    params,
    metadata,
):

    mlflow.set_registry_uri(
        "databricks-uc"
    )

    signature = infer_signature(
        input_example,
        model.predict(input_example),
    )

    with mlflow.start_run() as run:

        mlflow.log_params(params)

        mlflow.log_metrics(metrics)

        mlflow.log_dict(
            metadata,
            "model_metadata.json",
        )

        logged_model = (
            mlflow.sklearn.log_model(
                sk_model=model,
                name="model",
                signature=signature,
                input_example=input_example,
            )
        )

        version = mlflow.register_model(
            logged_model.model_uri,
            REGISTERED_MODEL_NAME,
        )

    return (
        version.version,
        run.info.run_id,
    )