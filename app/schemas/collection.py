from typing import Any, NotRequired, TypedDict

from pydantic import BaseModel, Field


class CollectionDetails(TypedDict):
    uuid: str
    name: str
    metadata: dict[str, Any]
    table_id: NotRequired[str]


class CollectionCreate(BaseModel):
    """Schema for creating a new collection."""

    name: str = Field(..., description="The unique name of the collection.")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Optional metadata for the collection."
    )


class CollectionUpdate(BaseModel):
    """Schema for updating an existing collection."""

    name: str | None = Field(None, description="New name for the collection.")
    metadata: dict[str, Any] | None = Field(
        None, description="Updated metadata for the collection."
    )


class CollectionResponse(BaseModel):
    uuid: str = Field(..., description="The unique identifier of the collection in PGVector.")
    name: str = Field(..., description="The name of the collection.")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Metadata associated with the collection."
    )

    class Config:
        from_attributes = True
