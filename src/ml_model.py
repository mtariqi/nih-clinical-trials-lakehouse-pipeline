import logging
from datetime import datetime

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, roc_auc_score, f1_score,
    mean_absolute_error, classification_report,
)
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sqlalchemy import create_engine

from config import (
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB,
    REDSHIFT_USER, REDSHIFT_PASSWORD,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME,
    DATA_PROCESSED_DIR,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FEATURES = [
    "phase", "lead_sponsor_class", "location_country",
    "enrollment_count", "study_duration_days", "sex",
    "healthy_volunteers",
]
TARGET = "is_completed"


def load_training_data() -> pd.DataFrame:
    """Load gold mart data from Redshift for training."""
    conn_str = (
        f"postgresql+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}"
        f"@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
    )
    engine = create_engine(conn_str)
    query = """
        SELECT
            phase,
            lead_sponsor_class,
            location_country,
            enrollment_count,
            avg_duration_days        AS study_duration_days,
            'all'                    AS sex,
            'no'                     AS healthy_volunteers,
            CASE WHEN completion_rate_pct > 50 THEN 1 ELSE 0 END AS is_completed
        FROM trial_enrollment_mart
        WHERE enrollment_count IS NOT NULL
          AND avg_duration_days IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} rows for training")
    return df


def preprocess(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Encode categoricals and return X, y arrays."""
    df = df.copy()
    df["enrollment_count"] = df["enrollment_count"].fillna(0).clip(upper=100_000)
    df["study_duration_days"] = df["study_duration_days"].fillna(365)

    cat_cols = ["phase", "lead_sponsor_class", "location_country", "sex", "healthy_volunteers"]
    for col in cat_cols:
        df[col] = LabelEncoder().fit_transform(df[col].fillna("unknown").astype(str))

    X = df[FEATURES].values
    y = df[TARGET].values
    return X, y


def train_and_register(run_date: str | None = None) -> dict:
    """Train model, log to MLflow, register if best performer. Returns metrics dict."""
    run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    df = load_training_data()
    X, y = preprocess(df)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    models = {
        "random_forest": RandomForestClassifier(
            n_estimators=200, max_depth=10, min_samples_leaf=5,
            class_weight="balanced", random_state=42, n_jobs=-1,
        ),
        "gradient_boosting": GradientBoostingClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.05,
            random_state=42,
        ),
    }

    best_auc = 0.0
    best_metrics = {}

    for model_name, clf in models.items():
        with mlflow.start_run(run_name=f"{model_name}_{run_date}"):
            mlflow.log_param("model_type", model_name)
            mlflow.log_param("run_date", run_date)
            mlflow.log_param("n_train", len(X_train))
            mlflow.log_param("n_test", len(X_test))
            mlflow.log_params(clf.get_params())

            clf.fit(X_train, y_train)
            y_pred = clf.predict(X_test)
            y_prob = clf.predict_proba(X_test)[:, 1]

            metrics = {
                "accuracy": accuracy_score(y_test, y_pred),
                "auc": roc_auc_score(y_test, y_prob),
                "f1": f1_score(y_test, y_pred, average="weighted"),
                "mae": mean_absolute_error(y_test, y_pred),
            }

            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(
                clf,
                artifact_path="model",
                registered_model_name=f"nih-trial-{model_name}",
            )

            logger.info(
                f"{model_name} — AUC: {metrics['auc']:.4f}, "
                f"F1: {metrics['f1']:.4f}, MAE: {metrics['mae']:.4f}"
            )

            if metrics["auc"] > best_auc:
                best_auc = metrics["auc"]
                best_metrics = metrics
                best_metrics["model"] = model_name

                # Save predictions for downstream use
                preds_df = pd.DataFrame({
                    "predicted_completed": y_pred,
                    "completion_probability": y_prob,
                    "actual": y_test,
                })
                out_path = f"{DATA_PROCESSED_DIR}/trial_predictions_{run_date}.csv"
                preds_df.to_csv(out_path, index=False)
                mlflow.log_artifact(out_path)

    logger.info(f"Best model: {best_metrics.get('model')} with AUC {best_auc:.4f}")
    return best_metrics


if __name__ == "__main__":
    metrics = train_and_register()
    print(metrics)
