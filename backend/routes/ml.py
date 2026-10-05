import joblib
import pandas as pd
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

# Local imports
from routes.auth import get_current_user
from database import get_db
from .network import get_grid_features 

router = APIRouter(
    prefix="/network/ml",
    tags=["Machine Learning"]
)

# ==========================================
# 1. GLOBAL LOAD (Run exactly once when server starts)
# ==========================================
MODEL_PATH = r"D:\phase_1_project\backend\routes\xgboost_drop_detector.pkl"

try:
    deployment_package = joblib.load(MODEL_PATH)
    xgb_model = deployment_package['model']
    safe_features = deployment_package['features']
    optimal_threshold = deployment_package['optimal_threshold']
    print(f"ML Model loaded successfully. Custom threshold: {optimal_threshold:.4f}")
except Exception as e:
    print(f"Warning: Could not load ML model. {e}")
    xgb_model, safe_features, optimal_threshold = None, None, 0.5


# ==========================================
# 2. PREDICTION ENDPOINT
# ==========================================
@router.get("/grid/{grid_id}/predict-anomaly")
def predict_grid_anomaly(
    grid_id: int,
    as_of: datetime | None = Query(None),
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user)
):
    # 1. Model Availability Check
    if xgb_model is None:
        raise HTTPException(
            status_code=500, 
            detail="Machine learning model is not loaded on the server."
        )

    # 2. Fetch pre-computed features from MySQL (via your robust function)
    try:
        grid_data = get_grid_features(
            grid_id=grid_id, 
            as_of=as_of, 
            db=db,
            current_user=current_user
        )
    except HTTPException as e:
        raise e  # Pass through the 404 if grid/features aren't found

    # 3. Data Freshness Check
    if grid_data["feature_freshness"]["status"] == "OUTDATED":
        raise HTTPException(
            status_code=400,
            detail="Features are too old to make a reliable prediction."
        )

    # 4. Format features strictly for XGBoost
    feature_dict = grid_data["features"]
    
    try:
        # Convert dictionary to DataFrame
        input_df = pd.DataFrame([feature_dict])
        print("\n--- DEBUGGING DATA SHAPE ---")
        print("Columns Pandas found:", input_df.columns.tolist())
        print("----------------------------\n")
        # Lock in the exact column order required by the model
        input_df = input_df[safe_features]
        
    except KeyError as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Database is missing features required by ML model: {e}"
        )

    # 5. Run Inference
    try:
        # predict_proba returns a 2D array, [:, 1] gets the anomaly probability
        prob = float(xgb_model.predict_proba(input_df)[:, 1][0])
        is_anomaly = bool(prob >= optimal_threshold)

        return {
            "grid_id": grid_id,
            "prediction_timestamp": feature_dict.get("feature_timestamp", feature_dict.get("timestamp")),
            "anomaly_predicted": is_anomaly,
            "drop_probability": round(prob, 4),
            "threshold_applied": float(round(optimal_threshold, 4))
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error during model inference: {str(e)}"
        )