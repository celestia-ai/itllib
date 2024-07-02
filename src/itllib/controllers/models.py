from pydantic import BaseModel
from typing import Optional


class Metadata(BaseModel):
    name: str
    fiber: Optional[str] = "resource"
    remote: Optional[str] = None


class ResourceConfig(BaseModel):
    apiVersion: str
    kind: str
    metadata: Metadata
