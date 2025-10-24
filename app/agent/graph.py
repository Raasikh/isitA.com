import os
from typing import Literal, TypedDict

from langchain.chat_models import init_chat_model
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from app.agent.state import AgentState
from app.core.config import settings

# Configuration
os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY
LLM_MODEL = "gpt-4o-mini"

llm = init_chat_model(LLM_MODEL)


class NewsState(TypedDict):
    """State for news classification workflow"""
    messages: list
    article_text: str
    category: str
    confidence: float
    needs_review: bool


CATEGORIES = [
    "politics",
    "business",
    "technology",
    "sports",
    "entertainment",
    "health",
    "science",
    "world"
]


async def classify_news_node(state: NewsState) -> dict:
    """Classify news article into categories"""
    article_text = state.get("article_text", "")

    if not article_text:
        last_message = state["messages"][-1].content if state["messages"] else ""
        article_text = last_message

    classification_prompt = f"""
You are a news classification expert. Classify the following article into ONE category.

Available categories: {", ".join(CATEGORIES)}

Article:
{article_text}

Respond in this exact format:
Category: [category name]
Confidence: [0.0-1.0]

If the article doesn't clearly fit any category or contains multiple topics, set confidence below 0.7.
"""

    messages = [SystemMessage(content=classification_prompt)]
    response = await llm.ainvoke(messages)

    # Parse response
    content = response.content.strip()
    category = "unknown"
    confidence = 0.0

    for line in content.split("\n"):
        if line.startswith("Category:"):
            category = line.split(":", 1)[1].strip().lower()
        elif line.startswith("Confidence:"):
            try:
                confidence = float(line.split(":", 1)[1].strip())
            except ValueError:
                confidence = 0.5

    return {
        **state,
        "category": category,
        "confidence": confidence,
        "needs_review": confidence < 0.7,
        "messages": [AIMessage(content=f"Classified as: {category} (confidence: {confidence:.2f})")],
    }


async def review_node(state: NewsState) -> dict:
    """Human review for low confidence classifications"""
    category = state.get("category", "unknown")
    confidence = state.get("confidence", 0.0)
    article_text = state.get("article_text", "")

    review_prompt = f"""
This article was classified as '{category}' with low confidence ({confidence:.2f}).

Article snippet:
{article_text[:500]}...

Available categories: {", ".join(CATEGORIES)}

Please review and provide:
1. Correct category
2. Reason for the classification

Format:
Category: [category]
Reason: [explanation]
"""

    messages = [SystemMessage(content=review_prompt)]
    response = await llm.ainvoke(messages)

    # Parse reviewed category
    content = response.content.strip()
    reviewed_category = category

    for line in content.split("\n"):
        if line.startswith("Category:"):
            reviewed_category = line.split(":", 1)[1].strip().lower()
            break

    return {
        **state,
        "category": reviewed_category,
        "confidence": 1.0,
        "needs_review": False,
        "messages": [AIMessage(content=response.content)],
    }


async def validate_category_node(state: NewsState) -> dict:
    """Validate that category is in allowed list"""
    category = state.get("category", "unknown")

    if category not in CATEGORIES:
        return {
            **state,
            "category": "unknown",
            "needs_review": True,
            "messages": [AIMessage(content=f"Invalid category '{category}'. Needs review.")],
        }

    return state


def route_after_classification(state: NewsState) -> Literal["validate", "review"]:
    """Route based on confidence"""
    needs_review = state.get("needs_review", False)

    if needs_review:
        return "review"
    return "validate"


def route_after_validation(state: NewsState) -> Literal["review", "END"]:
    """Route based on validation result"""
    needs_review = state.get("needs_review", False)

    if needs_review:
        return "review"
    return "END"


def builder() -> CompiledStateGraph:
    """Build news classification graph"""
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("classify", classify_news_node)
    graph.add_node("validate", validate_category_node)
    graph.add_node("review", review_node)

    # Add edges
    graph.add_edge(START, "classify")
    graph.add_conditional_edges(
        "classify",
        route_after_classification,
        {
            "validate": "validate",
            "review": "review"
        }
    )
    graph.add_conditional_edges(
        "validate",
        route_after_validation,
        {
            "review": "review",
            "END": END
        }
    )
    graph.add_edge("review", END)

    return graph.compile()


