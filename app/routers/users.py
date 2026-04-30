from fastapi import APIRouter, Depends

from app.dependencies import get_current_user
from app.models.user import User
from app.schemas.auth import UserResponse
from app.schemas.common import DataResponse

router = APIRouter()


@router.get("/me", response_model=DataResponse[UserResponse])
async def get_me(current_user: User = Depends(get_current_user)):
    return {"data": {"user_id": str(current_user.id), "email": current_user.email}}
