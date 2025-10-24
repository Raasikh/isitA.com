import json
import re

import yaml
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_mcp_adapters.client import MultiServerMCPClient

from app.core.app_state import app_state
from app.core.config import settings
from app.core.logging import logger
from app.services.embbedings import Collection, CollectionsManager


class Util:
    @staticmethod
    def get_mcp_client():
        """Connect to MCP server"""
        return MultiServerMCPClient(
            {
                settings.MCP_SERVER_NAME: {
                    "transport": settings.MCP_SERVER_TRANSPORT,
                    "url": f"http://{settings.MCP_SERVER_HOST}:{settings.MCP_SERVER_PORT}/mcp",
                }
            }
        )

    @staticmethod
    async def get_formatted_prompt(client, server_name: str, prompt_name: str, **kwargs) -> str:
        """Get prompt from MCP server."""
        prompt = await client.get_prompt(server_name=server_name, prompt_name=prompt_name)
        data = json.loads(prompt[0].content)
        return data["template"].format(**kwargs)

    @staticmethod
    async def get_resource_data(client, server_name: str, uri: str):
        """Get MCP server resource"""
        resources = await client.get_resources(server_name=server_name, uris=uri)
        if not resources:
            logger.error(f"No resource data found for URI: {uri}")
        return resources[0].data

    @staticmethod
    async def get_root_collection_by_name(name: str, system_id: str) -> Collection:
        manager = CollectionsManager(user_id=system_id)
        collections = await manager.list()
        col = next((c for c in collections if c["name"] == name), None)
        if not col:
            logger.error(f"System collection : '{name}' not found")
        return Collection(collection_id=col["uuid"], user_id=system_id)

    @staticmethod
    def clean_page_content_string(text: str) -> str:
        cleaned_text = text.replace("\\n", "\n")
        cleaned_text = cleaned_text.replace("\\\n", "")
        lines = [re.sub(r"\s+", " ", line).strip() for line in cleaned_text.split("\n")]
        return "\n".join(lines)

    @staticmethod
    def format_to_yaml(data, default_text: str) -> str:
        if not data:
            return default_text
        return yaml.dump(data, sort_keys=False, default_flow_style=False, indent=2).strip()

    @staticmethod
    async def stream_generator(input_messages: list, config: dict):
        """Yields server-sent events for each step of the graph's execution."""
        messages_as_objects = [HumanMessage(content=msg) for msg in input_messages]
        nodes_to_monitor = ["Text-to-SQL Agent", "triage", "llm_stream"]
        async for event in app_state.graph.astream_events(
            {"messages": messages_as_objects},
            config={
                **config,
                "callbacks": [app_state.langfuse_handler],
                "metadata": {
                    "langfuse_user_id": config["configurable"].get("user_id", "default"),
                    "langfuse_session_id": config["configurable"].get("thread_id", "default"),
                },
            },
            version="v2",
            include_names=nodes_to_monitor,
        ):
            event_name = event["event"]

            # Node start
            if event_name.endswith("_start"):
                node_name = event["name"]
                yield f"data: {json.dumps({'stage': node_name, 'status': 'running'})}\n\n"

            # Node End
            elif event_name.endswith("_end"):
                node_name = event["name"]
                output = event["data"].get("output")

                # Langgraph serialize
                serializable_result = Util.serialize_langgraph_output(output)
                yield f"data: {json.dumps({'stage': node_name, 'status': 'completed', 'result': serializable_result})}\n\n"

            # LLM Stream
            elif event_name == "on_chat_model_stream":
                chunk = event["data"]["chunk"]
                serializable_chunk = Util.serialize_langgraph_output(chunk)
                yield f"data: {json.dumps({'stage': 'llm_stream', 'chunk': serializable_chunk})}\n\n"

    @staticmethod
    def serialize_langgraph_output(output):
        """
        Serialize various LangGraph/LangChain output types to JSON-serializable format.
        """
        if output is None:
            return None

        # Handle LangChain messages (AIMessage, HumanMessage, SystemMessage, ToolMessage)
        if isinstance(output, BaseMessage):
            return {
                "type": output.__class__.__name__,
                "content": output.content,
                "additional_kwargs": output.additional_kwargs,
                "id": getattr(output, "id", None),
                "name": getattr(output, "name", None),
            }

        # Handle lists of messages (Langraph)
        if isinstance(output, list):
            return [Util.serialize_langgraph_output(item) for item in output]

        # Handle dictionaries
        if isinstance(output, dict):
            return {key: Util.serialize_langgraph_output(value) for key, value in output.items()}

        # Handle Pydantic models
        if hasattr(output, "model_dump"):
            try:
                return output.model_dump()
            except (TypeError, ValueError):
                return str(output)

        # Handle legacy Pydantic models
        if hasattr(output, "dict"):
            try:
                return output.dict()
            except (TypeError, ValueError):
                return str(output)

        try:
            json.dumps(output)
            return output
        except (TypeError, ValueError):
            return str(output)
