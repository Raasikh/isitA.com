from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.auth import get_enhanced_user
from app.schemas import CollectionCreate, CollectionResponse, CollectionUpdate
from app.services.embbedings import CollectionsManager

router = APIRouter()


@router.post(
    "",
    response_model=CollectionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def collections_create(
    collection_data: CollectionCreate,
    user: dict = Depends(get_enhanced_user),
):
    collection_info = await CollectionsManager(user["sub"]).create(
        collection_data.name, collection_data.metadata
    )
    if not collection_info:
        raise HTTPException(status_code=500, detail="Failed to create collection")
    return CollectionResponse(**collection_info)


@router.get("", response_model=list[CollectionResponse])
async def collections_list(
    user: dict = Depends(get_enhanced_user),
):
    return [CollectionResponse(**c) for c in await CollectionsManager(user["sub"]).list()]


@router.get("/{collection_id}", response_model=CollectionResponse)
async def collections_get(
    collection_id: UUID,
    user: dict = Depends(get_enhanced_user),
):
    collection = await CollectionsManager(user["sub"]).get(str(collection_id))
    if not collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Collection '{collection_id}' not found",
        )
    return CollectionResponse(**collection)


@router.delete("/{collection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def collections_delete(
    collection_id: UUID,
    user: dict = Depends(get_enhanced_user),
):
    await CollectionsManager(user["sub"]).delete(str(collection_id))
    return "Collection deleted successfully."


@router.patch("/{collection_id}", response_model=CollectionResponse)
async def collections_update(
    collection_id: UUID,
    collection_data: CollectionUpdate,
    user: dict = Depends(get_enhanced_user),
):
    updated_collection = await CollectionsManager(user["sub"]).update(
        str(collection_id),
        name=collection_data.name,
        metadata=collection_data.metadata,
    )

    if not updated_collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Failed to update collection '{collection_id}'",
        )

    return CollectionResponse(**updated_collection)
