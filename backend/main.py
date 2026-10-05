from fastapi import FastAPI, Depends, HTTPException, status
from routes.network import router as network_router
from routes.rule import router as rule_router
from routes.ml import router as ml_router
from routes.ai import router as ai_router
from routes.auth import get_password_hash, verify_password, create_access_token
from fastapi.security import OAuth2PasswordRequestForm
from database import get_db
from sqlalchemy import text
from sqlalchemy.orm import Session

app = FastAPI(
    title="Telecom Network Analytics API",
    version="1.0.0"
)

app.include_router(network_router)
app.include_router(rule_router)
app.include_router(ml_router)
app.include_router(ai_router)
@app.get("/health")
def health():
    return {
        "status": "healthy"
    }

@app.post("/login", tags=["Authentication"])
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    # 1. Fetch user from DB
    query = text("SELECT username, hashed_password FROM users WHERE username = :u")
    user = db.execute(query, {"u": form_data.username}).mappings().first()

    # 2. Verify existence and password
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 3. Generate JWT Token
    access_token = create_access_token(data={"sub": user["username"]})
    
    # Must return exactly this format for OAuth2 standard
    return {"access_token": access_token, "token_type": "bearer"}