import uuid as uuid_package

from sqlalchemy import JSON, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class CollectionStore(Base):
    __tablename__ = "langchain_pg_collection"

    uuid: Mapped[uuid_package.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid_package.uuid4
    )

    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    cmetadata: Mapped[dict] = mapped_column(JSON, nullable=True)

    # Establishes the one-to-many relationship from a collection to its embeddings.
    # 'passive_deletes=True' ensures that the database's ON DELETE CASCADE is used.
    embeddings = relationship("EmbeddingStore", back_populates="collection", passive_deletes=True)
