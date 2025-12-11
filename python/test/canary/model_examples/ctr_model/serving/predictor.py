from google.cloud.aiplatform.prediction.predictor import Predictor
from google.cloud.aiplatform.utils import prediction_utils
import xgboost as xgb
import pandas as pd
import joblib
import os

class CTRPredictor(Predictor):
    """Custom predictor for CTR model with dict input and score output"""
    
    def load(self, artifacts_uri: str):
        """Load the XGBoost model from artifacts
        
        Args:
            artifacts_uri: GCS path or local path to model artifacts
        """
        # Download artifacts if from GCS
        if artifacts_uri.startswith('gs://'):
            prediction_utils.download_model_artifacts(artifacts_uri)
            artifacts_uri = "." # Artifacts are now in the current directory
        
        # Load the model
        model_path = os.path.join(artifacts_uri, "model.joblib")
        self.model = joblib.load(model_path)
        
        print(f"Model loaded from {model_path}")
    
    def preprocess(self, prediction_input: dict) -> pd.DataFrame:
        """Convert dict instances to DataFrame with correct feature order
        
        Args:
            prediction_input: Dict with "instances" key containing list of dicts
                Example: {"instances": [{"user_id_click_event_average_7d": 0.5, ...}]}
            
        Returns:
            DataFrame with features in correct order
        """
        instances = prediction_input["instances"]
        
        # Convert to DataFrame
        df = pd.DataFrame(instances)
        
        # Ensure correct feature order (must match training)
        feature_order = [
            'user_id_click_event_average_7d',
            'listing_price_cents',
            'price_log',
            'price_bucket'
        ]
        
        return df[feature_order]
    
    def predict(self, instances: pd.DataFrame) -> list:
        """Run XGBoost prediction
        
        Args:
            instances: Preprocessed DataFrame
            
        Returns:
            List of prediction scores
        """
        dmatrix = xgb.DMatrix(instances.values)
        predictions = self.model.predict(dmatrix)
        return predictions.tolist()
    
    def postprocess(self, prediction_results: list) -> dict:
        """Format predictions as {"score": value}
        
        Args:
            prediction_results: List of raw prediction scores
            
        Returns:
            Dict with predictions formatted as list of {"score": value}
        """
        formatted_predictions = [
            {"score": float(score)} for score in prediction_results
        ]
        
        return {"predictions": formatted_predictions}

