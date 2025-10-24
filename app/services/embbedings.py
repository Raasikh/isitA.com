import builtins
import json
import uuid
from typing import Any, List, Optional

from fastapi import status
from fastapi.exceptions import HTTPException
from langchain_core.documents import Document
from loguru import logger

from app.core.database import get_db_connection, get_vectorstore
from app.schemas.collection import CollectionDetails

SYSTEM_OWNERS = {"system", "root"}


class CollectionsManager:
    def __init__(self, user_id: str) -> None:
        self.user_id = user_id

    @staticmethod
    async def setup() -> None:
        logger.info("Starting database initialization...")
        get_vectorstore()
        logger.info("Database initialization complete.")

    async def list(
        self,
    ) -> list[CollectionDetails]:
        async with get_db_connection() as conn:
            records = await conn.fetch(
                """
                SELECT uuid, cmetadata
                FROM langchain_pg_collection
                WHERE cmetadata->>'owner_id' = $1 OR cmetadata->>'owner_id' = ANY($2)
                ORDER BY cmetadata->>'name';
                """,
                self.user_id,
                list(SYSTEM_OWNERS),
            )

        result: list[CollectionDetails] = []
        for r in records:
            metadata = json.loads(r["cmetadata"])
            name = metadata.pop("name", "Unnamed")
            result.append(
                {
                    "uuid": str(r["uuid"]),
                    "name": name,
                    "metadata": metadata,
                }
            )
        return result

    async def get(
        self,
        collection_id: str,
    ) -> CollectionDetails | None:
        async with get_db_connection() as conn:
            rec = await conn.fetchrow(
                """
                SELECT uuid, name, cmetadata
                  FROM langchain_pg_collection
                 WHERE uuid = $1
                   AND (cmetadata->>'owner_id' = $2 OR cmetadata->>'owner_id' = ANY($3));
                """,
                collection_id,
                self.user_id,
                list(SYSTEM_OWNERS),
            )

        if not rec:
            return None

        metadata = json.loads(rec["cmetadata"])
        name = metadata.pop("name", "Unnamed")
        return {
            "uuid": str(rec["uuid"]),
            "name": name,
            "metadata": metadata,
            "table_id": rec["name"],
        }

    async def create(
        self,
        collection_name: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> CollectionDetails | None:
        metadata = metadata.copy() if metadata else {}
        metadata["owner_id"] = self.user_id
        metadata["name"] = collection_name

        table_id = str(uuid.uuid4())

        get_vectorstore(table_id, collection_metadata=metadata)

        async with get_db_connection() as conn:
            rec = await conn.fetchrow(
                """
                SELECT uuid, name, cmetadata
                  FROM langchain_pg_collection
                 WHERE name = $1
                   AND cmetadata->>'owner_id' = $2;
                """,
                table_id,
                self.user_id,
            )
        if not rec:
            return None
        metadata = json.loads(rec["cmetadata"])
        name = metadata.pop("name")
        return {"uuid": str(rec["uuid"]), "name": name, "metadata": metadata}

    async def update(
        self,
        collection_id: str,
        *,
        name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> CollectionDetails:
        if metadata is None and name is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Must update at least 1 attribute.",
            )

        async with get_db_connection() as conn:
            collection = await conn.fetchrow(
                """
                SELECT cmetadata->>'owner_id' AS owner_id
                FROM langchain_pg_collection
                WHERE uuid = $1;
                """,
                collection_id,
            )
            if not collection:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Collection '{collection_id}' not found.",
                )
            collection_owner = collection["owner_id"]

        # Block modification of system collections by non-system users
        if collection_owner in SYSTEM_OWNERS and self.user_id not in SYSTEM_OWNERS:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Modification of system collections is not allowed.",
            )

        if metadata is not None:
            merged = metadata.copy()
            merged["owner_id"] = self.user_id

            if name is not None:
                merged["name"] = name
            else:
                existing = await self.get(collection_id)
                if not existing:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Collection '{collection_id}' not found or not owned by you.",
                    )
                merged["name"] = existing["name"]

            metadata_json = json.dumps(merged)

            async with get_db_connection() as conn:
                rec = await conn.fetchrow(
                    """
                    UPDATE langchain_pg_collection
                       SET cmetadata = $1::jsonb
                     WHERE uuid = $2
                       AND cmetadata->>'owner_id' = $3
                    RETURNING uuid, cmetadata;
                    """,
                    metadata_json,
                    collection_id,
                    self.user_id,
                )

        else:
            async with get_db_connection() as conn:
                rec = await conn.fetchrow(
                    """
                    UPDATE langchain_pg_collection
                       SET cmetadata = jsonb_set(
                             cmetadata::jsonb,
                             '{name}',
                             to_jsonb($1::text),
                             true
                           )
                     WHERE uuid = $2
                       AND cmetadata->>'owner_id' = $3
                    RETURNING uuid, cmetadata;
                    """,
                    name,
                    collection_id,
                    self.user_id,
                )

        if not rec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{collection_id}' not found or not owned by you.",
            )

        full_meta = json.loads(rec["cmetadata"])
        friendly_name = full_meta.pop("name", "Unnamed")

        return {
            "uuid": str(rec["uuid"]),
            "name": friendly_name,
            "metadata": full_meta,
        }

    async def delete(
        self,
        collection_id: str,
    ) -> int:
        async with get_db_connection() as conn:
            collection = await conn.fetchrow(
                """
                SELECT cmetadata->>'owner_id' AS owner_id
                FROM langchain_pg_collection
                WHERE uuid = $1;
                """,
                collection_id,
            )
            if not collection:
                return 0
            collection_owner = collection["owner_id"]

        # Block deletion of system collections by non-system users
        if collection_owner in SYSTEM_OWNERS and self.user_id not in SYSTEM_OWNERS:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Deletion of system collections is not allowed.",
            )

        async with get_db_connection() as conn:
            result = await conn.execute(
                """
                DELETE FROM langchain_pg_collection
                 WHERE uuid = $1
                   AND cmetadata->>'owner_id' = $2;
                """,
                collection_id,
                self.user_id,
            )
        return int(result.split()[-1])


class Collection:
    def __init__(self, collection_id: str, user_id: str) -> None:
        self.collection_id = collection_id
        self.user_id = user_id

    async def _get_details_or_raise(self) -> dict[str, Any]:
        details = await CollectionsManager(self.user_id).get(self.collection_id)
        if not details:
            raise HTTPException(status_code=404, detail="Collection not found")
        return details

    async def upsert(self, documents: list[Document]) -> list[str]:
        details = await self._get_details_or_raise()
        store = get_vectorstore(collection_name=details["table_id"])
        added_ids = store.add_documents(documents)
        return added_ids

    async def delete(
        self,
        *,
        file_id: Optional[str] = None,
    ) -> bool:
        async with get_db_connection() as conn:
            collection_info = await conn.fetchrow(
                """
                SELECT lpc.cmetadata->>'owner_id' AS owner_id
                FROM langchain_pg_collection AS lpc
                WHERE lpc.uuid = $1;
                """,
                self.collection_id,
            )

            if not collection_info:
                raise HTTPException(status_code=404, detail="Collection not found.")

            collection_owner = collection_info["owner_id"]

            if collection_owner in SYSTEM_OWNERS and self.user_id not in SYSTEM_OWNERS:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Deletion of files from system collections is not allowed.",
                )

            delete_sql = """
                DELETE FROM langchain_pg_embedding AS lpe
                USING langchain_pg_collection AS lpc
                WHERE lpe.collection_id   = lpc.uuid
                  AND lpc.uuid             = $1
                  AND lpc.cmetadata->>'owner_id' = $2
                  AND lpe.cmetadata->>'file_id'   = $3
            """

            result = await conn.execute(
                delete_sql,
                self.collection_id,
                self.user_id,
                file_id,
            )

            deleted_count = int(result.split()[-1])
            logger.warning("")
            logger.info(f"Deleted {deleted_count} embeddings for file {file_id!r}.")

            if deleted_count == 0:
                await self._get_details_or_raise()
        return True

    async def list(self, *, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]:
        async with get_db_connection() as conn:
            rows = await conn.fetch(
                """
                WITH UniqueFileChunks AS (
                  SELECT -- DISTINCT ON (lpe.cmetadata->>'file_id')
                         lpe.id,
                         lpe.cmetadata->>'file_id' AS file_id
                    FROM langchain_pg_embedding lpe
                    JOIN langchain_pg_collection lpc
                      ON lpe.collection_id = lpc.uuid
                   WHERE lpc.uuid = $1
                     AND (lpc.cmetadata->>'owner_id' = $2 OR lpc.cmetadata->>'owner_id' = ANY($3))
                     AND lpe.cmetadata->>'file_id' IS NOT NULL
                   ORDER BY lpe.cmetadata->>'file_id', lpe.id
                )
                SELECT emb.id,
                       emb.document,
                       emb.cmetadata
                FROM langchain_pg_embedding AS emb
                JOIN UniqueFileChunks AS ufc
                  ON emb.id = ufc.id
                ORDER BY ufc.file_id
                LIMIT  $4
                OFFSET $5
                """,
                self.collection_id,
                self.user_id,
                list(SYSTEM_OWNERS),
                limit,
                offset,
            )

        docs: list[dict[str, Any]] = []
        for r in rows:
            metadata = json.loads(r["cmetadata"]) if r["cmetadata"] else {}
            docs.append(
                {
                    "id": str(r["id"]),
                    "content": r["document"],
                    "metadata": metadata,
                    "collection_id": str(self.collection_id),
                }
            )

        if not docs:
            await self._get_details_or_raise()
        return docs

    async def get(self, document_id: str) -> dict[str, Any]:
        async with get_db_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT e.uuid, e.document, e.cmetadata
                  FROM langchain_pg_embedding e
                  JOIN langchain_pg_collection c
                    ON e.collection_id = c.uuid
                 WHERE e.uuid = $1
                   AND (c.cmetadata->>'owner_id' = $2 OR c.cmetadata->>'owner_id' = ANY($3))
                   AND c.uuid = $4
                """,
                document_id,
                self.user_id,
                list(SYSTEM_OWNERS),
                self.collection_id,
            )
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")

        metadata = json.loads(row["cmetadata"]) if row["cmetadata"] else {}
        return {
            "id": str(row["uuid"]),
            "content": row["document"],
            "metadata": metadata,
        }

    async def search(self, query: str, *, limit: int = 4) -> builtins.list[dict[str, Any]]:
        details = await self._get_details_or_raise()
        store = get_vectorstore(collection_name=details["table_id"])
        results = store.similarity_search_with_score(query, k=limit)
        return [
            {
                "id": doc.id,
                "page_content": doc.page_content,
                "metadata": doc.metadata,
                "score": score,
            }
            for doc, score in results
        ]

    async def search_min(self, query: str, *, limit: int = 4) -> List[str]:
        details = await self._get_details_or_raise()
        store = get_vectorstore(collection_name=details["table_id"])
        results = store.similarity_search_with_score(query, k=limit)
        return [doc.page_content for doc, score in results]
