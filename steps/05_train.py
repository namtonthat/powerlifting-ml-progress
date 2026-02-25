"""
Machine Learning Training
"""

import logging
import math
import os
from datetime import datetime

import conf
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import optuna
import pandas as pd
import polars as pl
import seaborn as sns
import xgboost as xgb
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# MLFlow configurations
mlflow.set_tracking_uri("sqlite:///mlruns.db")

load_dotenv(conf.env_file_path)
# NOTE: MLFLOW_TRACKING_URI and MLFLOW_S3_ENDPOINT_URL are set by the load
# Use MINIO_ROOT_USER / ROOT_PASSWORD as S3 credentials
os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("MINIO_ROOT_USER")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("MINIO_ROOT_PASSWORD")

MAX_TRIALS = 250


# override Optuna's default logging to ERROR only
optuna.logging.set_verbosity(optuna.logging.INFO)


def get_or_create_experiment(experiment_name):
    """
    Retrieve the ID of an existing MLflow experiment or create a new one if it doesn't exist.

    This function checks if an experiment with the given name exists within MLflow.
    If it does, the function returns its ID. If not, it creates a new experiment
    with the provided name and returns its ID.

    Parameters:
    - experiment_name (str): Name of the MLflow experiment.

    Returns:
    - str: ID of the existing or newly created MLflow experiment.
    """

    if experiment := mlflow.get_experiment_by_name(experiment_name):
        return experiment.experiment_id

    try:
        logging.info("Creating new experiment: %s with artifact location: %s", experiment_name, conf.artifact_location)
        return mlflow.create_experiment(experiment_name, artifact_location=conf.artifact_location)
    except mlflow.exceptions.MlflowException:
        logging.info("Creating new experiment: %s without artifact store", experiment_name)
        return mlflow.create_experiment(experiment_name)


def champion_callback(study, frozen_trial):
    """
    Logging callback that will report when a new trial iteration improves upon existing
    best trial values.

    Note: This callback is not intended for use in distributed computing systems such as Spark
    or Ray due to the micro-batch iterative implementation for distributing trials to a cluster's
    workers or agents.
    The race conditions with file system state management for distributed trials will render
    inconsistent values with this callback.
    """

    winner = study.user_attrs.get("winner", None)

    if study.best_value and winner != study.best_value:
        study.set_user_attr("winner", study.best_value)
        if winner:
            improvement_percent = (abs(winner - study.best_value) / study.best_value) * 100
            logging.info(f"Trial {frozen_trial.number} achieved value: {frozen_trial.value} with {improvement_percent: .4f}% improvement")
        else:
            logging.info(f"Initial trial {frozen_trial.number} achieved value: {frozen_trial.value}")


def objective(trial):
    trial_name = f"trial_{trial.number:03d}"
    with mlflow.start_run(nested=True, run_name=trial_name):
        params = {
            "objective": "reg:squarederror",
            "eval_metric": "rmse",
            "booster": "gbtree",
            "lambda": trial.suggest_float("lambda", 1e-8, 1.0, log=True),
            "alpha": trial.suggest_float("alpha", 1e-8, 1.0, log=True),
        }

        if params["booster"] in ("gbtree", "dart"):
            params["max_depth"] = trial.suggest_int("max_depth", 3, 12)
            params["eta"] = trial.suggest_float("eta", 1e-8, 1.0, log=True)
            params["gamma"] = trial.suggest_float("gamma", 1e-8, 1.0, log=True)
            params["grow_policy"] = trial.suggest_categorical("grow_policy", ["depthwise", "lossguide"])
            params["subsample"] = trial.suggest_float("subsample", 0.5, 1.0)
            params["colsample_bytree"] = trial.suggest_float("colsample_bytree", 0.3, 1.0)
            params["min_child_weight"] = trial.suggest_int("min_child_weight", 1, 30)

        n_rounds = trial.suggest_int("num_boost_round", 200, 2000, step=100)

        pruning_callback = optuna.integration.XGBoostPruningCallback(trial, "validation-rmse")
        bst = xgb.train(params, dtrain, num_boost_round=n_rounds, evals=[(dval, "validation")], early_stopping_rounds=50, callbacks=[pruning_callback], verbose_eval=False)
        preds = bst.predict(dval)
        error = mean_squared_error(y_val, preds)
        val_rmse = math.sqrt(error)
        val_mae = mean_absolute_error(y_val, preds)
        val_r2 = r2_score(y_val, preds)

        mlflow.log_params(params)
        mlflow.log_param("num_boost_round", n_rounds)
        mlflow.log_param("n_features", dtrain.num_col())
        mlflow.log_metric("mse", error)
        mlflow.log_metric("rmse", val_rmse)
        mlflow.log_metric("mae", val_mae)
        mlflow.log_metric("r2", val_r2)
        mlflow.set_tag("booster", params["booster"])
        mlflow.set_tag("trial_number", trial.number)

    return error


def plot_feature_importance(model, booster):
    """|
    Plots feature importance for an XGBoost model.

    Args:
    - model: A trained XGBoost model

    Returns:
    - fig: The matplotlib figure object
    """
    fig, ax = plt.subplots(figsize=(10, 8))
    importance_type = "weight" if booster == "gblinear" else "gain"
    xgb.plot_importance(
        model,
        importance_type=importance_type,
        ax=ax,
        title=f"Feature Importance based on {importance_type}",
    )
    plt.tight_layout()
    plt.close(fig)

    return fig


def plot_residuals(model, dvalid, valid_y, save_path=None):
    """
    Plots the residuals of the model predictions against the true values.

    Args:
    - model: The trained XGBoost model.
    - dvalid (xgb.DMatrix): The validation data in XGBoost DMatrix format.
    - valid_y (pd.Series): The true values for the validation set.
    - save_path (str, optional): Path to save the generated plot. If not specified, plot won't be saved.

    Returns:
    - None (Displays the residuals plot on a Jupyter window)
    """

    # Predict using the model
    preds = model.predict(dvalid)

    # Calculate residuals
    residuals = valid_y - preds

    # Set Seaborn style
    sns.set_style("whitegrid", {"axes.facecolor": "#c2c4c2", "grid.linewidth": 1.5})

    # Create scatter plot
    fig = plt.figure(figsize=(12, 8))
    plt.scatter(valid_y, residuals, color="blue", alpha=0.5)
    plt.axhline(y=0, color="r", linestyle="-")

    # Set labels, title and other plot properties
    plt.title("Residuals vs True Values", fontsize=18)
    plt.xlabel("True Values", fontsize=16)
    plt.ylabel("Residuals", fontsize=16)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.grid(axis="y")

    plt.tight_layout()

    # Save the plot if save_path is specified
    if save_path:
        plt.savefig(save_path, format="png", dpi=600)

    # Show the plot
    plt.close(fig)

    return fig


if __name__ == "__main__":
    RANDOM_SEED = 7
    FEATURE_SET_VERSION = 5
    TARGET = "total_progress"
    columns_to_exclude = ["name", "date", TARGET]

    # Load data
    logging.info("Loading data")
    base_df = pl.read_parquet(conf.base_local_file_path)

    # Filter: drop rows without progress (first comp per lifter) and outliers
    base_df = base_df.filter(pl.col(TARGET).is_not_null() & pl.col(TARGET).is_finite() & (pl.col(TARGET).abs() < 200))

    modelling_cols = [
        "name",
        "date",
        "bodyweight",
        "age_class",
        "sex",
        "ipf_weight_class",
        "federation",
        "parent_federation",
        "time_since_last_comp_years",
        "bodyweight_change",
        "cumulative_comps",
        "tenure",
        "meet_type",
        "is_origin_country",
        "prev_squat_ratio",
        "prev_bench_ratio",
        "prev_deadlift_ratio",
        "prev_rolling_avg_total_3",
        "prev_rolling_avg_squat_3",
        "prev_rolling_avg_bench_3",
        "prev_rolling_avg_deadlift_3",
        "prev_total_percentile_rank",
        "prev_total_per_bw",
        "segment_mean_total",
        "prev_total_vs_segment_mean",
        "previous_squat",
        "previous_bench",
        "previous_deadlift",
        "previous_total",
        "previous_wilks",
        "elo_rating",
        "elo_change",
        "meet_field_elo",
        TARGET,
    ]

    modelling_df = base_df.select(modelling_cols)

    # Namings
    run_name = f"total_progress_v{FEATURE_SET_VERSION}"
    today_date = datetime.today().strftime("%Y-%m-%d")
    experiment_id = get_or_create_experiment(today_date)

    # Set the current active MLflow experiment
    mlflow.set_experiment(experiment_id=experiment_id)

    # --- Temporal train/val/test split ---
    dates = modelling_df["date"]
    sorted_dates = dates.sort()
    n = len(sorted_dates)
    train_cutoff = sorted_dates[int(n * conf.TEMPORAL_TRAIN_CUTOFF)]
    val_cutoff = sorted_dates[int(n * conf.TEMPORAL_VAL_CUTOFF)]

    train_mask = dates <= train_cutoff
    val_mask = (dates > train_cutoff) & (dates <= val_cutoff)
    test_mask = dates > val_cutoff

    train_df = modelling_df.filter(train_mask)
    val_df = modelling_df.filter(val_mask)
    test_df = modelling_df.filter(test_mask)

    logging.info("Temporal split — train: %d, val: %d, test: %d", len(train_df), len(val_df), len(test_df))
    logging.info("Train cutoff: %s, Val cutoff: %s", train_cutoff, val_cutoff)

    # Prepare features
    def prepare_Xy(df):
        X = df.select(pl.exclude(columns_to_exclude)).to_pandas()
        for col in X.select_dtypes(include="object").columns:
            X[col] = X[col].astype("category")
        y = df.select([TARGET]).to_pandas()
        return X, y

    X_train, y_train = prepare_Xy(train_df)
    X_val, y_val = prepare_Xy(val_df)
    X_test, y_test = prepare_Xy(test_df)

    # Baseline: predict training mean
    baseline_pred = y_train.mean().values[0]
    baseline_mse = mean_squared_error(y_test, [baseline_pred] * len(y_test))
    baseline_rmse = math.sqrt(baseline_mse)
    logging.info("Baseline RMSE (predict mean): %.4f", baseline_rmse)

    # Convert to DMatrix
    dtrain = xgb.DMatrix(X_train, label=y_train, enable_categorical=True)
    dval = xgb.DMatrix(X_val, label=y_val, enable_categorical=True)
    dtest = xgb.DMatrix(X_test, label=y_test, enable_categorical=True)

    # Initiate the parent run and call the hyperparameter tuning child run logic
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name, nested=True):
        # Initialize the Optuna study
        study = optuna.create_study(direction="minimize", pruner=optuna.pruners.MedianPruner(n_warmup_steps=50))

        # Execute the hyperparameter optimization trials.
        study.optimize(objective, n_trials=MAX_TRIALS, callbacks=[champion_callback])

        n_pruned = len([t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED])
        n_complete = len([t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE])
        logging.info("Optuna done — %d complete, %d pruned out of %d trials", n_complete, n_pruned, len(study.trials))

        mlflow.log_params(study.best_params)
        mlflow.log_metric("best_mse", study.best_value)
        mlflow.log_metric("best_rmse", math.sqrt(study.best_value))
        mlflow.log_metric("baseline_rmse", baseline_rmse)
        mlflow.log_metric("n_pruned_trials", n_pruned)
        mlflow.log_metric("n_complete_trials", n_complete)

        # Retrain final model on train+val with early stopping on val
        best_params = {k: v for k, v in study.best_params.items() if k != "num_boost_round"}
        best_params["objective"] = "reg:squarederror"
        best_params["eval_metric"] = "rmse"
        best_n_rounds = study.best_params.get("num_boost_round", 1000)

        X_train_val = pd.concat([X_train, X_val], ignore_index=True)
        y_train_val = pd.concat([y_train, y_val], ignore_index=True)
        dtrain_full = xgb.DMatrix(X_train_val, label=y_train_val, enable_categorical=True)
        # For final model, use the best n_rounds directly (already tuned with early stopping)
        model = xgb.train(best_params, dtrain_full, num_boost_round=best_n_rounds)

        # Compute metrics on held-out test set
        test_preds = model.predict(dtest)
        test_mse = mean_squared_error(y_test, test_preds)
        test_rmse = math.sqrt(test_mse)
        test_mae = mean_absolute_error(y_test, test_preds)
        test_r2 = r2_score(y_test, test_preds)
        test_median_ae = float(np.median(np.abs(y_test.values.ravel() - test_preds)))

        mlflow.log_metrics(
            {
                "test_mse": test_mse,
                "test_rmse": test_rmse,
                "test_mae": test_mae,
                "test_r2": test_r2,
                "test_median_ae": test_median_ae,
                "baseline_rmse": baseline_rmse,
                "rmse_lift_over_baseline": baseline_rmse - test_rmse,
                "n_train": len(X_train),
                "n_val": len(X_val),
                "n_test": len(X_test),
                "n_features": len(X_train.columns),
            }
        )

        logging.info("=" * 60)
        logging.info("FINAL MODEL RESULTS")
        logging.info("=" * 60)
        logging.info("Test RMSE: %.4f | MAE: %.4f | Median AE: %.4f | R²: %.4f", test_rmse, test_mae, test_median_ae, test_r2)
        logging.info("Baseline RMSE: %.4f | RMSE lift: %.4f", baseline_rmse, baseline_rmse - test_rmse)
        logging.info("Features: %d | Train: %d | Val: %d | Test: %d", len(X_train.columns), len(X_train), len(X_val), len(X_test))

        # Log tags
        mlflow.set_tags(
            tags={
                "project": "powerlifting-ml-progress",
                "optimizer_engine": "optuna",
                "model_family": "xgboost",
                "feature_set_version": FEATURE_SET_VERSION,
                "target": TARGET,
                "min_competitions": conf.MIN_COMPETITIONS,
                "min_days_between_comps": conf.MIN_DAYS_BETWEEN_COMPS,
                "split_method": "temporal",
                "train_cutoff": str(train_cutoff),
                "val_cutoff": str(val_cutoff),
                "best_booster": study.best_params.get("booster"),
                "best_n_rounds": study.best_params.get("num_boost_round"),
            }
        )

        # Feature importance analysis — log top features to help guide feature selection
        importance_scores = model.get_score(importance_type="gain")
        if importance_scores:
            sorted_features = sorted(importance_scores.items(), key=lambda x: x[1], reverse=True)
            logging.info("-" * 60)
            logging.info("TOP FEATURES BY GAIN:")
            for rank, (feat, gain) in enumerate(sorted_features[:15], 1):
                logging.info("  %2d. %-35s gain=%.1f", rank, feat, gain)
                mlflow.log_metric(f"feature_gain_{feat}", gain)

            # Log feature list as a tag for easy filtering
            top_5_features = ",".join(f[0] for f in sorted_features[:5])
            mlflow.set_tag("top_5_features", top_5_features)

            # Log features with zero importance (candidates for removal)
            all_features = set(X_train.columns)
            used_features = set(importance_scores.keys())
            unused_features = all_features - used_features
            if unused_features:
                logging.info("UNUSED FEATURES (zero gain): %s", ", ".join(sorted(unused_features)))
                mlflow.set_tag("unused_features", ",".join(sorted(unused_features)))

        # Per-segment RMSE evaluation
        logging.info("-" * 60)
        logging.info("PER-SEGMENT RMSE:")
        for sex_val in X_test["sex"].cat.categories:
            mask = X_test["sex"] == sex_val
            if mask.sum() >= 50:
                seg_rmse = math.sqrt(mean_squared_error(y_test[mask], test_preds[mask]))
                seg_r2 = r2_score(y_test[mask], test_preds[mask])
                mlflow.log_metric(f"rmse_sex_{sex_val}", seg_rmse)
                mlflow.log_metric(f"r2_sex_{sex_val}", seg_r2)
                logging.info("  sex=%s (n=%d): RMSE=%.4f R²=%.4f", sex_val, mask.sum(), seg_rmse, seg_r2)

        wc_counts = X_test["ipf_weight_class"].value_counts()
        top_wcs = wc_counts[wc_counts >= 50].index
        for wc_val in top_wcs:
            mask = X_test["ipf_weight_class"] == wc_val
            seg_rmse = math.sqrt(mean_squared_error(y_test[mask], test_preds[mask]))
            seg_r2 = r2_score(y_test[mask], test_preds[mask])
            mlflow.log_metric(f"rmse_wc_{str(wc_val).replace('+', '_plus')}", seg_rmse)
            mlflow.log_metric(f"r2_wc_{str(wc_val).replace('+', '_plus')}", seg_r2)
            logging.info("  wc=%s (n=%d): RMSE=%.4f R²=%.4f", wc_val, mask.sum(), seg_rmse, seg_r2)

        logging.info("=" * 60)

        # Log artifacts (plots + model) — requires artifact store (MinIO)
        try:
            importances = plot_feature_importance(model, booster=study.best_params.get("booster"))
            mlflow.log_figure(figure=importances, artifact_file="feature_importances.png")

            residuals = plot_residuals(model, dtest, y_test)
            mlflow.log_figure(figure=residuals, artifact_file="residuals.png")

            artifact_path = "model"
            mlflow.xgboost.log_model(
                xgb_model=model,
                artifact_path=artifact_path,
                input_example=X_train.iloc[[0]],
                model_format="ubj",
                metadata={"model_data_version": FEATURE_SET_VERSION},
            )
            model_uri = mlflow.get_artifact_uri(artifact_path)
        except Exception as e:
            logging.warning("Artifact logging failed (is MinIO running?): %s", e)
