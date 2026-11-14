from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.engines.project import LATE_EPOCH_POLICIES
from app.services import project_service as svc
from app.services.project_service import BUMP_POLICIES

router = APIRouter(tags=["epoch"])


class PolicyBody(BaseModel):
    late_epoch_policy: str | None = None
    epoch_bump_buffer_policy: str | None = None


class BumpBody(BaseModel):
    stream_id: int
    to_epoch: int | None = None
    buffer_policy: str | None = None
    note: str | None = None


class ReplayBody(BaseModel):
    stream_id: int
    bypass_id: int | None = None
    event_id: str | None = None
    payload: dict | None = None
    allow_overwrite_newer: bool = False


@router.get("/epoch")
def epoch_panel():
    return svc.epoch_panel()


@router.get("/epoch/policies")
def epoch_policies():
    return {
        "late_epoch_policies": list(LATE_EPOCH_POLICIES),
        "epoch_bump_buffer_policies": list(BUMP_POLICIES),
    }


@router.get("/epoch/changes")
def epoch_changes(stream_id: int | None = None, limit: int = 50):
    return svc.list_epoch_changes(stream_id=stream_id, limit=limit)


@router.get("/epoch/bypass")
def list_bypass(stream_id: int | None = None, origin: str | None = None, status: str | None = None):
    return svc.list_bypass(stream_id=stream_id, origin=origin, status=status)


@router.get("/epoch/replay-log")
def replay_log(stream_id: int | None = None, limit: int = 100):
    return svc.list_replay_log(stream_id=stream_id, limit=limit)


@router.post("/epoch/bump")
def bump(body: BumpBody):
    try:
        return svc.bump_epoch(
            body.stream_id,
            to_epoch=body.to_epoch,
            buffer_policy=body.buffer_policy,
            note=body.note,
        )
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.post("/epoch/policies/{stream_id}")
def set_policies(stream_id: int, body: PolicyBody):
    try:
        return svc.set_stream_policies(
            stream_id,
            late_epoch_policy=body.late_epoch_policy,
            epoch_bump_buffer_policy=body.epoch_bump_buffer_policy,
        )
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.post("/epoch/replay")
def replay(body: ReplayBody):
    try:
        return svc.replay_bypass(
            body.stream_id,
            bypass_id=body.bypass_id,
            event_id=body.event_id,
            payload_override=body.payload,
            allow_overwrite_newer=body.allow_overwrite_newer,
        )
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


class SeedBufferBody(BaseModel):
    stream_id: int
    count: int = 2


@router.post("/epoch/seed-buffer")
def seed_buffer(body: SeedBufferBody):
    try:
        return svc.seed_old_buffer(body.stream_id, count=body.count)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
