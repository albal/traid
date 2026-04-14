"""
Docker container/image management routes — /api/containers, /api/images, /api/docker
"""

import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import Literal

from api import uds_client
from api.models import JobAccepted

router = APIRouter(tags=["docker"])

_DOCKER_ID_RE             = re.compile(r"^[a-f0-9A-F]{1,64}$")
_DOCKER_IMAGE_RE          = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_./@:-]{1,254}$")
_DOCKER_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$")
_DOCKER_PORT_RE           = re.compile(r"^(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:)?\d{1,5}:\d{1,5}(?:/(?:tcp|udp))?$")
_DOCKER_ENV_RE            = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}=[^\n\r;&|`$<>]{0,512}$")


class ContainerActionRequest(BaseModel):
    action: Literal["start", "stop", "rm"]


class ContainerCreateRequest(BaseModel):
    image:    str
    name:     str = ""
    ports:    list[str] = []
    restart:  Literal["no", "always", "unless-stopped", "on-failure"] = "no"
    env_vars: list[str] = []

    @field_validator("image")
    @classmethod
    def _image(cls, v):
        if not _DOCKER_IMAGE_RE.match(v):
            raise ValueError("invalid image name/tag")
        return v

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        if v and not _DOCKER_CONTAINER_NAME_RE.match(v):
            raise ValueError("invalid container name")
        return v

    @field_validator("ports")
    @classmethod
    def _ports(cls, v):
        if len(v) > 20:
            raise ValueError("too many port mappings (max 20)")
        for p in v:
            if not _DOCKER_PORT_RE.match(p):
                raise ValueError(f"invalid port mapping: {p!r}")
        return v

    @field_validator("env_vars")
    @classmethod
    def _env_vars(cls, v):
        if len(v) > 50:
            raise ValueError("too many env vars (max 50)")
        for e in v:
            if not _DOCKER_ENV_RE.match(e):
                raise ValueError(f"invalid env var: {e!r}")
        return v


class PullImageRequest(BaseModel):
    image: str

    @field_validator("image")
    @classmethod
    def _img(cls, v):
        if not _DOCKER_IMAGE_RE.match(v):
            raise ValueError("invalid image name/tag")
        return v


async def _send(action: str, params: dict = {}) -> dict:
    try:
        return await uds_client.send_request(action, params)
    except uds_client.WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except uds_client.WorkerError as exc:
        status = 400 if exc.code == "VALIDATION_ERROR" else 500
        raise HTTPException(status_code=status, detail={"code": exc.code, "message": exc.message})


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------

@router.get("/api/containers")
async def list_containers(all: bool = True):
    data = await _send("docker_list_containers", {"all": all})
    return data.get("containers", []) if isinstance(data, dict) else data


@router.post("/api/containers", status_code=201)
async def create_container(request: ContainerCreateRequest):
    return await _send("docker_create_container", {
        "image":    request.image,
        "name":     request.name,
        "ports":    request.ports,
        "restart":  request.restart,
        "env_vars": request.env_vars,
    })


@router.post("/api/containers/{container_id}/action")
async def container_action(container_id: str, request: ContainerActionRequest):
    if not _DOCKER_ID_RE.match(container_id):
        raise HTTPException(status_code=400, detail="invalid container ID")
    return await _send("docker_container_action",
                        {"container_id": container_id, "action": request.action})


@router.get("/api/containers/{container_id}/logs")
async def container_logs(container_id: str, lines: int = 200):
    if not _DOCKER_ID_RE.match(container_id):
        raise HTTPException(status_code=400, detail="invalid container ID")
    if not (1 <= lines <= 10000):
        raise HTTPException(status_code=400, detail="lines must be 1–10000")
    return await _send("docker_container_logs",
                        {"container_id": container_id, "lines": lines})


# ---------------------------------------------------------------------------
# Images
# ---------------------------------------------------------------------------

@router.get("/api/images")
async def list_images():
    data = await _send("docker_list_images")
    return data.get("images", []) if isinstance(data, dict) else data


@router.post("/api/images/pull", status_code=202, response_model=JobAccepted)
async def pull_image(request: PullImageRequest):
    data = await _send("docker_pull_image", {"image": request.image})
    return JobAccepted(**data)


@router.delete("/api/images/{image_id}")
async def remove_image(image_id: str, force: bool = False):
    if not _DOCKER_ID_RE.match(image_id):
        raise HTTPException(status_code=400, detail="invalid image ID")
    return await _send("docker_remove_image", {"image_id": image_id, "force": force})


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------

@router.post("/api/docker/prune", status_code=202, response_model=JobAccepted)
async def system_prune():
    data = await _send("docker_system_prune")
    return JobAccepted(**data)
