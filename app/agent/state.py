from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph import add_messages


class AgentState(TypedDict, total=False):
    messages: Annotated[List[BaseMessage], add_messages]
    decision: Optional[str]
    clarification_count: Optional[int]
    retry_count: Optional[int]

    user_id: str
    thread_id: str
    memory_agent: Optional[Any]

    #Memory Context