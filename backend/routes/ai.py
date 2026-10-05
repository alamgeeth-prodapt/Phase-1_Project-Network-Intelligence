from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel

from database import get_db
from .auth import get_current_user
from ai.agent import chat
from ai.plugins.registry import default_registry


router = APIRouter(
    prefix="/ai",
    tags=["AI NOC Assistant"],
)


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = None
    enabled_plugins: Optional[List[str]] = None


class ChatResponse(BaseModel):
    response: str
    traces: List[Dict[str, Any]] = []
    duration_ms: float = 0.0
    active_plugins: List[str] = []
    compliance_passed: bool = True


class PluginInfoResponse(BaseModel):
    name: str
    display_name: str
    description: str
    version: str
    is_enabled: bool
    tools_count: int


@router.get(
    "/plugins",
    response_model=List[PluginInfoResponse],
    summary="List all registered NOC agent plugins and their active status",
)
def list_plugins(
    current_user: str = Depends(get_current_user),
):
    """Return all modular plugins currently loaded in the assistant."""
    return default_registry.list_plugins()


@router.post(
    "/chat",
    response_model=ChatResponse,
    summary="Submit query to the hook-governed, plugin-augmented AI NOC Assistant",
)
def ai_chat(
    request: ChatRequest,
    db: Session = Depends(get_db),
    current_user: str = Depends(get_current_user),
):
    try:
        history_list = None
        if request.history:
            history_list = [{"role": m.role, "content": m.content} for m in request.history]

        result = chat(
            user_message=request.message,
            db=db,
            history=history_list,
            enabled_plugins=request.enabled_plugins,
            context={"current_user": current_user},
        )

        return ChatResponse(
            response=result["response"],
            traces=result.get("traces", []),
            duration_ms=result.get("duration_ms", 0.0),
            active_plugins=result.get("active_plugins", []),
            compliance_passed=result.get("compliance_passed", True),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"AI NOC Assistant operational failure: {str(exc)}"
        )