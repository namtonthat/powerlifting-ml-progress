"""
Machine Learning Training
"""
from datetime import datetime
import matplotlib.pyplot as plt
import math
import seaborn as sns
import polars as pl
import conf
import os
import logging
from dotenv import load_dotenv

## Perform XG Boost on data
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import optuna
import mlflow


# MLFlow configurations
mlflow.set_tracking_uri("sqlite:///mlruns.db")

load_dotenv(conf.env_file_path)
# NOTE: MLFLOW_TRACKING_URI and MLFLOW_S3_ENDPOINT_URL are set by the load
# Use MINIO_ROOT_USER / ROOT_PASSWORD as S3 credentials
os.environ["AWS_ACCESS_KEY_ID"] = os.environ.get("MINIO_ROOT_USER")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("MINIO_ROOT_PASSWORD")

MAX_TRIALS = 250


# override Optuna's default logging to ERROR only
optuna.logging.set_verbosity(optuna.logging.ERROR)


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
    else:
        try:
            logging.info("Creating new experiment: %s with artifact location: %s", experiment_name, conf.artifact_location)
            mlflow.create_experiment(experiment_name, artifact_location=conf.artifact_location)
        except MlflowException as e:
            print(e)

        logging.info("Creating new experiment: %s without artifact store", experiment_name)
        standard_experiment = mlflow.create_experiment(experiment_name)
        return standard_experiment

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
            print(f"Trial {frozen_trial.number} achieved value: {frozen_trial.value} with " f"{improvement_percent: .4f}% improvement")
        else:
            print(f"Initial trial {frozen_trial.number} achieved value: {frozen_trial.value}")


def objective(trial):
    with mlflow.start_run(nested=True):
        # Define hyperparameters
        params = {
            "objective": "reg:squarederror",
            "eval_metric": "rmse",
            "booster": trial.suggest_categorical("booster", ["gbtree", "gblinear", "dart"]),
            "lambda": trial.suggest_float("lambda", 1e-8, 1.0, log=True),
            "alpha": trial.suggest_float("alpha", 1e-8, 1.0, log=True),
        }

        if params["booster"] == "gbtree" or params["booster"] == "dart":
            params["max_depth"] = trial.suggest_int("max_depth", 1, 9)
            params["eta"] = trial.suggest_float("eta", 1e-8, 1.0, log=True)
            params["gamma"] = trial.suggest_float("gamma", 1e-8, 1.0, log=True)
            params["grow_policy"] = trial.suggest_categorical("grow_policy", ["depthwise", "lossguide"])

        # Train XGBoost model
        bst = xgb.train(params, dtrain)
        preds = bst.predict(dtest)
        error = mean_squared_error(y_test, preds)

        # Log to MLflow
        mlflow.log_params(params)
        mlflow.log_metric("mse", error)
        mlflow.log_metric("rmse", math.sqrt(error))

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
    test_size = 0.33
    columns_to_exclude = ["name", "total", "date"]

    # Load data
    logging.info("Loading data")
    base_df = pl.read_parquet(conf.base_local_file_path)


    modelling_cols = [
        "name",
        "date",
        "bodyweight",
        "age_class",
        "sex",
        "total",
        # "time_since_last_comp",
        # "bodyweight_change",
        "time_since_last_comp",
        "bodyweight_change",
        "cumulative_comps",
        "meet_type",
        "previous_squat",
        "previous_bench",
        "previous_deadlift",
        "previous_total",
    ]

    modelling_df = base_df.select(modelling_cols)

    # Namings
    run_name = "first_attempt"
    today_date = datetime.today().strftime("%Y-%m-%d")
    experiment_id = get_or_create_experiment(today_date)

    # Set the current active MLflow experiment
    mlflow.set_experiment(experiment_id=experiment_id)

    # standardise data for XG Boost
    pre_X = modelling_df.select(pl.exclude(columns_to_exclude)).to_pandas()
    # need to convert object columns to categorical
    X = pre_X
    for col in X.select_dtypes(include="object").columns:
        X[col] = X[col].astype("category")

    y = modelling_df.select(["total"]).to_pandas()

    # split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=RANDOM_SEED)

    # convert data into DMatrix format
    dtrain = xgb.DMatrix(X_train, label=y_train, enable_categorical=True)
    dtest = xgb.DMatrix(X_test, y_test, enable_categorical=True)

    # define a logging callback that will report on only new challenger parameter configurations if a
    # trial has usurped the state of 'best conditions'   # Initiate the parent run and call the hyperparameter tuning child run logic
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name, nested=True):
        # Initialize the Optuna study
        study = optuna.create_study(direction="minimize")

        # Execute the hyperparameter optimization trials.
        # Note the addition of the `champion_callback` inclusion to control our logging
        study.optimize(objective, n_trials=MAX_TRIALS, callbacks=[champion_callback])


        mlflow.log_params(study.best_params)
        mlflow.log_metric("best_mse", study.best_value)
        mlflow.log_metric("best_rmse", math.sqrt(study.best_value))

        # Log tags
        mlflow.set_tags(
            tags={
                "project": "powerlifting-ml-progress",
                "optimizer_engine": "optuna",
                "model_family": "xgboost",
                "feature_set_version": 1,
            }
        )

        # Log a fit model instance
        model = xgb.train(study.best_params, dtrain)

        # Log the correlation plot
        # mlflow.log_figure(figure=correlation_plot, artifact_file="correlation_plot.png")

        # Log the feature importances plot
        importances = plot_feature_importance(model, booster=study.best_params.get("booster"))
        mlflow.log_figure(figure=importances, artifact_file="feature_importances.png")

        # Log the residuals plot
        # residuals = plot_residuals(model, dtest, y_test)
        # mlflow.log_figure(figure=residuals, artifact_file="residuals.png")

        artifact_path = "model"

        mlflow.xgboost.log_model(
            xgb_model=model,
            artifact_path=artifact_path,
            input_example=X_train.iloc[[0]],
            model_format="ubj",
            metadata={"model_data_version": 1},
        )

        # Get the logged model uri so that we can load it from the artifact store
        model_uri = mlflow.get_artifact_uri(artifact_path)

    """
    NOTES
    Use of MLFlow to track experiments
    XG Boost - Tree based modelling
    Low bias - high variance -> overfitting
    High bias - low variance -> underfitting
    High bias - high variance -> underfitting
    Low bias - low variance -> good fit
    k fold validation - split data into k folds, train on k-1 folds and test on the remaining fold
    k fold validation is used to tune hyperparameters
    can adjust XG boost parameters -> ask ChatGPT
    """
