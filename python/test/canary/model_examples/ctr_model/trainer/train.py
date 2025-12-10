"""
Test XGBoost model example to test model training + deployment
"""

import argparse
import json
import numpy as np
import pandas as pd
import xgboost as xgb
from typing import Dict, List, Tuple
import os
from pathlib import Path

from google.cloud import bigquery
import os


def generate_synthetic_training_data(n_samples: int = 1000, seed: int = 42) -> Tuple[pd.DataFrame, List[int]]:
    """
    Generate synthetic training data with 4 simple features

    Args:
        n_samples: Number of samples to generate
        seed: Random seed for reproducibility

    Returns:
        Tuple of (X_train DataFrame, y_train list)
    """
    np.random.seed(seed)

    X_train_data = []
    y_train = []

    for i in range(n_samples):
        # Generate features directly
        user_id_click_event_average_7d = np.random.uniform(0, 1)  # Average click rate
        listing_price_cents = np.random.choice([
            np.random.randint(500, 2000),
            np.random.randint(5000, 15000),
            np.random.randint(20000, 100000)
        ])
        price_log = np.log1p(listing_price_cents)

        # Price bucket
        if listing_price_cents < 1000:
            price_bucket = 0
        elif listing_price_cents < 5000:
            price_bucket = 1
        elif listing_price_cents < 10000:
            price_bucket = 2
        elif listing_price_cents < 50000:
            price_bucket = 3
        else:
            price_bucket = 4

        sample = {
            'user_id_click_event_average_7d': user_id_click_event_average_7d,
            'listing_price_cents': int(listing_price_cents),
            'price_log': price_log,
            'price_bucket': price_bucket
        }

        X_train_data.append(sample)

        # Generate label based on simple rules
        prob = 0.1  # Base probability
        if user_id_click_event_average_7d > 0.5:
            prob += 0.3
        if 1000 < listing_price_cents < 20000:
            prob += 0.25
        if price_bucket in [1, 2]:
            prob += 0.15

        label = np.random.binomial(1, min(prob, 0.9))
        y_train.append(label)

    X_train_df = pd.DataFrame(X_train_data)
    return X_train_df, y_train

def get_training_data_df(zipline_ctr_label_table: str = 'canary-443022.data.gcp_demo_derivations_v1__2', start_ds: str = '2025-10-03', end_ds: str = '2025-10-03') -> Tuple[pd.DataFrame, List[int]]:
    client = bigquery.Client()
    # Note: If your environment doesn't imply a project, explicitly pass it: bigquery.Client(project="your-project-id")

    # 2. Define your Query
    sql_query = f"""
        SELECT user_id_click_event_average_7d, listing_id_price_cents AS listing_price_cents, price_log, price_bucket, label
        FROM ${zipline_ctr_label_table} where ds BETWEEN '{start_ds}' AND '{end_ds}'
    """

    # 3. Run the query and convert to DataFrame
    df = client.query(sql_query).to_dataframe()

    # Split to X_train_df, y_train
    x_train_df = df.drop(columns=['label'])
    y_train = df['label'].tolist()
    return x_train_df, y_train



def train_model(
    X_train: pd.DataFrame,
    y_train: List[int],
    max_depth: int = 4,
    eta: float = 0.1,
    num_boost_round: int = 50,
    seed: int = 42
) -> xgb.Booster:
    """
    Train XGBoost model

    Args:
        X_train: DataFrame with training features
        y_train: List of binary labels
        max_depth: Maximum tree depth
        eta: Learning rate
        num_boost_round: Number of boosting rounds
        seed: Random seed

    Returns:
        Trained XGBoost model
    """
    print(f"Training with {len(X_train.columns)} features: {list(X_train.columns)}")

    # Train XGBoost model
    params = {
        'max_depth': max_depth,
        'eta': eta,
        'objective': 'binary:logistic',
        'eval_metric': 'logloss',
        'seed': seed
    }

    dtrain = xgb.DMatrix(X_train.values, label=y_train)
    print(f"Training model with params: {params}")

    model = xgb.train(params, dtrain, num_boost_round=num_boost_round)

    print(f"Training complete!")

    return model


def save_model(model: xgb.Booster, output_path: str):
    """Save model in Vertex AI compatible format"""
    import joblib

    # GCSFuse conversion for Vertex AI
    # When running in Vertex AI, gs:// paths are mounted at /gcs/
    gs_prefix = 'gs://'
    gcsfuse_prefix = '/gcs/'
    if output_path.startswith(gs_prefix):
        output_path = output_path.replace(gs_prefix, gcsfuse_prefix)

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save XGBoost model using joblib format
    model_file = output_dir / "model.joblib"
    joblib.dump(model, str(model_file))
    print(f"Model saved to {model_file}")

    # Save model metadata
    metadata = {
        'num_features': 4,
        'feature_names': ['user_id_click_event_average_7d', 'listing_price_cents', 'price_log', 'price_bucket'],
        'model_type': 'xgboost',
        'version': '1.0'
    }

    metadata_file = output_dir / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"Metadata saved to {metadata_file}")


def main():
    parser = argparse.ArgumentParser(description='Train XGBoost CTR model')

    # Data parameters
    parser.add_argument('--n-samples', type=int, default=1000,
                        help='Number of training samples to generate')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')

    # Model hyperparameters
    parser.add_argument('--max-depth', type=int, default=4,
                        help='Maximum tree depth')
    parser.add_argument('--eta', type=float, default=0.1,
                        help='Learning rate')
    parser.add_argument('--num-boost-round', type=int, default=50,
                        help='Number of boosting rounds')

    # Output parameters
    parser.add_argument('--model-dir', type=str,
                        default=os.environ.get('AIP_MODEL_DIR', './model_output'),
                        help='Directory to save the model (defaults to AIP_MODEL_DIR for Vertex AI)')

    args = parser.parse_args()

    print("="*80)
    print("XGBoost CTR Model Training")
    print("="*80)
    print(f"Configuration:")
    print(f"  Training samples: {args.n_samples}")
    print(f"  Max depth: {args.max_depth}")
    print(f"  Learning rate: {args.eta}")
    print(f"  Boosting rounds: {args.num_boost_round}")
    print(f"  Model output: {args.model_dir}")
    print(f"  Random seed: {args.seed}")
    print("="*80)

    # Generate training data
    # print("\n1. Generating synthetic training data...")
    # X_train, y_train = generate_synthetic_training_data(
    #     n_samples=args.n_samples,
    #     seed=args.seed
    # )
    # print(f"   Generated {len(X_train)} samples")
    # print(f"   Positive class ratio: {sum(y_train)/len(y_train):.2%}")

    X_train, y_train = get_training_data_df()
    # Train model
    print("\n2. Training model...")
    model = train_model(
        X_train,
        y_train,
        max_depth=args.max_depth,
        eta=args.eta,
        num_boost_round=args.num_boost_round,
        seed=args.seed
    )

    # Save model
    print("\n3. Saving model...")
    save_model(model, args.model_dir)

    # Quick validation
    print("\n4. Validation...")
    X_test = X_train.iloc[[0]]
    dmatrix = xgb.DMatrix(X_test)
    pred = model.predict(dmatrix)[0]
    print(f"   Sample prediction: {pred:.4f}")
    print(f"   Actual label: {y_train[0]}")

    print("\n" + "="*80)
    print("Training complete!")
    print("="*80)


if __name__ == "__main__":
    main()
