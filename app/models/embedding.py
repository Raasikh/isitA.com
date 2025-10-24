import uuid

from pgvector.sqlalchemy import Vector
from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class EmbeddingStore(Base):
    __tablename__ = "langchain_pg_embedding"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    # The vector column with a specified dimension size.
    embedding: Mapped[list[float]] = mapped_column(Vector(1536))
    document: Mapped[str] = mapped_column(String, nullable=True)
    cmetadata: Mapped[dict] = mapped_column(JSONB, nullable=True)

    # --- RELATIONSHIPS ---
    # Foreign key to link this embedding back to its parent collection.
    # ondelete="CASCADE" ensures that when a collection is deleted, all its
    # embeddings are automatically deleted by the database.
    collection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("langchain_pg_collection.uuid", ondelete="CASCADE")
    )
    collection = relationship("CollectionStore", back_populates="embeddings")

    # --- INDEXES ---
    __table_args__ = (
        # GIN index for efficient searching within the 'cmetadata' JSONB column.
        # This is great for filtering by metadata.
        Index(
            "ix_cmetadata_gin",
            "cmetadata",
            postgresql_using="gin",
            postgresql_ops={"cmetadata": "jsonb_path_ops"},
        ),
        # HNSW index for fast vector similarity search on 'embedding'.
        # Choose the operator class based on your model:
        # - 'vector_l2_ops' (<=>) → Euclidean (e.g., text-embedding-ada-002)
        # - 'vector_ip_ops' (<#>) → Inner Product
        # - 'vector_cosine_ops' (<=>) → Cosine (e.g., sentence-transformers)
        Index(
            "langchain_pg_embedding_embedding_idx",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )
