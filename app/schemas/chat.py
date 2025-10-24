from pydantic import BaseModel


class ChatRequest(BaseModel):
    messages: list[str]
    thread_id: str
    user_id: str = None
    session_id: str = None


class ChatResponse(BaseModel):
    response: str
    sql_query: str = None
    thread_id: str
    session_id: str
    timestamp: str
