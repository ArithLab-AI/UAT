import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, String, Text, Boolean
from sqlalchemy.orm import relationship

from app.db.database import Base


class DataChatSession(Base):
    """A conversation thread scoped to a single dataset."""

    __tablename__ = "data_chat_sessions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)  # uploaded | merged
    is_clean = Column(Boolean, nullable=False, default=False)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    dataset_name = Column(String(255), nullable=False)
    title = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    created_by = relationship("User")


class DataChatMessage(Base):
    """Every natural-language query and its generated SQL / result are stored here."""

    __tablename__ = "data_chat_messages"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    session_id = Column(
        String(36),
        ForeignKey("data_chat_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    nl_query = Column(Text, nullable=False)
    generated_sql = Column(Text, nullable=True)
    assistant_text = Column(Text, nullable=True)
    chart_spec = Column(JSON, nullable=True)
    result_preview = Column(JSON, nullable=True)  # capped list of result rows for history
    row_count = Column(Integer, nullable=True)
    status = Column(String(20), nullable=False, default="success")  # success | error | clarify
    error_message = Column(Text, nullable=True)
    attempts = Column(Integer, nullable=False, default=1)
    tokens_used = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    session = relationship("DataChatSession", backref="messages")
    created_by = relationship("User")
