from fastapi import APIRouter, HTTPException

from app.services import project_service as svc

router = APIRouter(tags=["streams"])


@router.get("/streams")
def list_streams():
    return svc.list_streams()


@router.get("/streams/{stream_id}")
def get_stream(stream_id: int):
    try:
        s = svc.get_stream(stream_id)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    if not s:
        raise HTTPException(404, "stream not found")
    return s
