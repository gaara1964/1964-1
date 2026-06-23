import asyncio
import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import discord
import httpx
from discord.ext import commands
from dotenv import load_dotenv
from openai import AsyncOpenAI


load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
OWNER_IDS = {int(x.strip()) for x in os.getenv("OWNER_IDS", "").split(",") if x.strip().isdigit()}
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
MCP_GUILD_ID = os.getenv("MCP_GUILD_ID") or os.getenv("GUILD_ID", "")
DISCORD_MCP_URL = os.getenv("DISCORD_MCP_URL", "").strip()
AI_AGENT_PREFIX = os.getenv("AI_AGENT_PREFIX", "!")

AI_API_KEY = os.getenv("AI_API_KEY") or os.getenv("OPENAI_API_KEY", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1")
AI_MODEL = os.getenv("AI_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")


SYSTEM_PROMPT = """
You are the private owner-only AI control agent for a Discord server.

The owner speaks naturally. Your job is to understand the request, choose MCP tools
or local bot tools, execute only what is needed, and report a concise result.

Rules:
- Only help the configured owner.
- For destructive actions such as delete category/channel/webhook, mass role changes,
  bans, kicks, or purges, ask for confirmation unless the owner explicitly confirms
  in the same message.
- Prefer exact Discord MCP tools when available.
- Include guildId in MCP arguments when a tool schema supports it and the owner did
  not provide another guild.
- If the request is ambiguous, ask one short clarifying question.
- Never invent tool results.
- Do not reveal API keys or bot tokens.

Return only JSON in this shape:
{
  "thought": "short private planning note",
  "tool_calls": [
    {"tool": "tool_name", "arguments": {"key": "value"}}
  ],
  "final": "message to send to the owner after executing tools, or a question if no tools should be called"
}

If no tool is needed, use an empty tool_calls array.
"""


def compact_json(value: Any, max_chars: int = 5000) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...truncated..."


def extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if match:
            return json.loads(match.group(0))
        raise


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]
    source: str


class McpHttpClient:
    """
    Small JSON-RPC client for common HTTP/SSE MCP bridges.

    Set DISCORD_MCP_URL to the MCP server's HTTP endpoint. If your MCP server uses
    a different transport, tell Codex the exact endpoint format and this adapter can
    be adjusted.
    """

    def __init__(self, url: str):
        self.url = url
        self.client = httpx.AsyncClient(timeout=45)
        self.session_id: Optional[str] = None

    async def close(self):
        await self.client.aclose()

    async def rpc(self, method: str, params: Optional[dict[str, Any]] = None) -> Any:
        request_id = str(uuid.uuid4())
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if self.session_id:
            headers["mcp-session-id"] = self.session_id
        async with self.client.stream("POST", self.url, json=payload, headers=headers) as response:
            response.raise_for_status()
            if response.headers.get("mcp-session-id"):
                self.session_id = response.headers["mcp-session-id"]
            if response.status_code == 202:
                return None
            content_type = response.headers.get("content-type", "")
            if "text/event-stream" in content_type:
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        data = line[5:].strip()
                        if data:
                            result = json.loads(data)
                            break
                else:
                    return None
            else:
                body = (await response.aread()).decode("utf-8").strip()
                if not body:
                    return None
                result = json.loads(body)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result.get("result")

    async def initialize(self):
        try:
            return await self.rpc(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "manager-ai-mcp-bot", "version": "1.0.0"},
                },
            )
        except Exception:
            return None

    async def initialized(self):
        try:
            await self.rpc("notifications/initialized")
        except Exception:
            pass

    async def list_tools(self) -> list[ToolSpec]:
        result = await self.rpc("tools/list", {})
        tools = result.get("tools", []) if isinstance(result, dict) else []
        return [
            ToolSpec(
                name=tool.get("name", ""),
                description=tool.get("description", ""),
                input_schema=tool.get("inputSchema", {}),
                source="mcp",
            )
            for tool in tools
            if tool.get("name")
        ]

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        return await self.rpc("tools/call", {"name": name, "arguments": arguments})


class OwnerAgentBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.messages = True
        intents.message_content = True
        super().__init__(command_prefix=AI_AGENT_PREFIX, intents=intents)
        self.ai = AsyncOpenAI(api_key=AI_API_KEY, base_url=AI_BASE_URL) if AI_API_KEY else None
        self.mcp = McpHttpClient(DISCORD_MCP_URL) if DISCORD_MCP_URL else None
        self.tool_cache: list[ToolSpec] = []
        self.history: dict[int, list[dict[str, str]]] = {}

    async def setup_hook(self):
        if self.mcp:
            await self.mcp.initialize()
            await self.mcp.initialized()
            try:
                self.tool_cache = await self.mcp.list_tools()
                print(f"Loaded {len(self.tool_cache)} MCP tools")
            except Exception as exc:
                print(f"Could not list MCP tools: {exc}")
        self.tool_cache.extend(local_tool_specs())

    async def close(self):
        if self.mcp:
            await self.mcp.close()
        await super().close()

    async def on_ready(self):
        print(f"AI MCP agent logged in as {self.user} ({self.user.id})")
        if not OWNER_IDS:
            print("WARNING: OWNER_IDS is empty. The bot will ignore all users.")
        if not DISCORD_MCP_URL:
            print("WARNING: DISCORD_MCP_URL is empty. MCP tools are disabled.")
        if not AI_API_KEY:
            print("WARNING: AI_API_KEY/OPENAI_API_KEY is empty. AI agent cannot think.")

    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.author.id not in OWNER_IDS:
            return
        if not self.should_handle(message):
            return
        content = self.clean_owner_message(message)
        if not content:
            return
        async with message.channel.typing():
            reply = await self.handle_owner_request(message, content)
        await safe_send(message.channel, reply)

    def should_handle(self, message: discord.Message) -> bool:
        if isinstance(message.channel, discord.DMChannel):
            return True
        if self.user and self.user in message.mentions:
            return True
        return message.content.strip().startswith(AI_AGENT_PREFIX)

    def clean_owner_message(self, message: discord.Message) -> str:
        content = message.content.strip()
        if self.user:
            content = content.replace(f"<@{self.user.id}>", "").replace(f"<@!{self.user.id}>", "")
        if content.startswith(AI_AGENT_PREFIX):
            content = content[len(AI_AGENT_PREFIX):]
        return content.strip()

    async def handle_owner_request(self, message: discord.Message, content: str) -> str:
        if not self.ai:
            return "AI is not configured. Add `AI_API_KEY`, `AI_BASE_URL`, and `AI_MODEL` in `.env`."

        channel_id = message.channel.id
        history = self.history.setdefault(channel_id, [])
        history.append({"role": "user", "content": content})
        history[:] = history[-10:]

        context = {
            "owner_id": message.author.id,
            "current_guild_id": message.guild.id if message.guild else GUILD_ID,
            "configured_mcp_guild_id": MCP_GUILD_ID,
            "current_channel_id": message.channel.id,
        }
        tool_catalog = self.render_tools_for_ai()
        plan = await self.ask_ai(history, context, tool_catalog)
        calls = plan.get("tool_calls", [])
        if not isinstance(calls, list):
            calls = []

        results = []
        for call in calls[:8]:
            name = call.get("tool")
            arguments = call.get("arguments") or {}
            if not isinstance(arguments, dict):
                arguments = {}
            if MCP_GUILD_ID and "guildId" not in arguments and self.tool_accepts_guild_id(name):
                arguments["guildId"] = MCP_GUILD_ID
            result = await self.execute_tool(name, arguments, message)
            results.append({"tool": name, "arguments": arguments, "result": result})

        success_results = [r for r in results if "error" not in str(r.get("result", "")).lower()]

        if success_results:
            final = await self.summarize_results(history, context, plan, results)
        else:
            final = str(plan.get("final") or "I need more detail before I can act.")
        history.append({"role": "assistant", "content": final})
        history[:] = history[-10:]
        return final

    def render_tools_for_ai(self) -> str:
        compact = []
        for tool in self.tool_cache:
            compact.append(
                {
                    "name": tool.name,
                    "source": tool.source,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                }
            )
        return compact_json(compact, 18000)

    def tool_accepts_guild_id(self, name: Optional[str]) -> bool:
        if not name:
            return False
        tool = next((t for t in self.tool_cache if t.name == name), None)
        schema_text = json.dumps(tool.input_schema) if tool else ""
        return "guildId" in schema_text

    async def ask_ai(self, history: list[dict[str, str]], context: dict[str, Any], tool_catalog: str) -> dict[str, Any]:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Context:\n{compact_json(context)}\n\n"
                    f"Available tools:\n{tool_catalog}\n\n"
                    f"Conversation:\n{compact_json(history)}"
                ),
            },
        ]
        response = await self.ai.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
            temperature=0.15,
            max_tokens=2500,
        )
        text = response.choices[0].message.content or "{}"
        try:
            return extract_json(text)
        except Exception:
            return {"tool_calls": [], "final": text}

    async def summarize_results(
        self,
        history: list[dict[str, str]],
        context: dict[str, Any],
        plan: dict[str, Any],
        results: list[dict[str, Any]],
    ) -> str:
        response = await self.ai.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Summarize tool execution results for the Discord server owner. Be concise and honest. Do not expose secrets.",
                },
                {
                    "role": "user",
                    "content": (
                        f"Context:\n{compact_json(context)}\n\n"
                        f"Original AI plan:\n{compact_json(plan)}\n\n"
                        f"Tool results:\n{compact_json(results, 12000)}\n\n"
                        f"Recent conversation:\n{compact_json(history)}"
                    ),
                },
            ],
            temperature=0.2,
            max_tokens=1200,
        )
        return response.choices[0].message.content or "Done."

    async def execute_tool(self, name: Optional[str], arguments: dict[str, Any], message: discord.Message) -> Any:
        if not name:
            return {"error": "missing tool name"}
        local = LOCAL_TOOLS.get(name)
        if local:
            return await local(self, message, arguments)
        if self.mcp:
            try:
                return await self.mcp.call_tool(name, arguments)
            except Exception as exc:
                return {"error": str(exc)}
        return {"error": "MCP endpoint is not configured"}


async def safe_send(channel: discord.abc.Messageable, content: str):
    content = content or "Done."
    chunks = [content[i : i + 1900] for i in range(0, len(content), 1900)]
    for chunk in chunks:
        await channel.send(chunk)


def local_tool_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="local_send_message",
            source="local",
            description="Send a message to a Discord channel by ID using this bot directly.",
            input_schema={
                "type": "object",
                "properties": {
                    "channel_id": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["channel_id", "content"],
            },
        ),
        ToolSpec(
            name="local_create_text_channel",
            source="local",
            description="Create a text channel in the current guild using this bot directly.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category_id": {"type": "string"},
                    "topic": {"type": "string"},
                },
                "required": ["name"],
            },
        ),
        ToolSpec(
            name="local_create_role",
            source="local",
            description="Create a role in the current guild using this bot directly.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "color": {"type": "string", "description": "Hex color like #ff0000"},
                },
                "required": ["name"],
            },
        ),
        ToolSpec(
            name="local_assign_role",
            source="local",
            description="Assign a role to a member by user ID and role name or role ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "role_id": {"type": "string"},
                    "role_name": {"type": "string"},
                },
                "required": ["user_id"],
            },
        ),
        ToolSpec(
            name="local_setup_welcome",
            source="local",
            description="Configure a welcome channel and message for new members. Use {member} and {server} placeholders.",
            input_schema={
                "type": "object",
                "properties": {
                    "channel_id": {"type": "string"},
                    "message": {"type": "string"},
                },
                "required": ["channel_id", "message"],
            },
        ),
    ]


async def local_send_message(bot: OwnerAgentBot, _message: discord.Message, args: dict[str, Any]) -> Any:
    channel = bot.get_channel(int(args["channel_id"]))
    if not channel or not hasattr(channel, "send"):
        return {"error": "channel not found"}
    await channel.send(str(args["content"]))
    return {"ok": True}


async def local_create_text_channel(bot: OwnerAgentBot, message: discord.Message, args: dict[str, Any]) -> Any:
    guild = message.guild or bot.get_guild(GUILD_ID)
    if not guild:
        return {"error": "guild not found"}
    category = None
    if args.get("category_id"):
        found = guild.get_channel(int(args["category_id"]))
        if isinstance(found, discord.CategoryChannel):
            category = found
    channel = await guild.create_text_channel(str(args["name"]), category=category, topic=args.get("topic"))
    return {"id": channel.id, "name": channel.name}


async def local_create_role(bot: OwnerAgentBot, message: discord.Message, args: dict[str, Any]) -> Any:
    guild = message.guild or bot.get_guild(GUILD_ID)
    if not guild:
        return {"error": "guild not found"}
    color = discord.Color.default()
    if args.get("color"):
        color = discord.Color(int(str(args["color"]).lstrip("#"), 16))
    role = await guild.create_role(name=str(args["name"]), color=color)
    return {"id": role.id, "name": role.name}


async def local_assign_role(bot: OwnerAgentBot, message: discord.Message, args: dict[str, Any]) -> Any:
    guild = message.guild or bot.get_guild(GUILD_ID)
    if not guild:
        return {"error": "guild not found"}
    member = guild.get_member(int(args["user_id"]))
    if not member:
        return {"error": "member not found"}
    role = None
    if args.get("role_id"):
        role = guild.get_role(int(args["role_id"]))
    if not role and args.get("role_name"):
        role = discord.utils.get(guild.roles, name=str(args["role_name"]))
    if not role:
        return {"error": "role not found"}
    await member.add_roles(role, reason="Owner AI agent requested role assignment")
    return {"ok": True, "member": member.id, "role": role.id}


async def local_setup_welcome(bot: OwnerAgentBot, _message: discord.Message, args: dict[str, Any]) -> Any:
    bot.welcome_channel_id = int(args["channel_id"])
    bot.welcome_message = str(args["message"])
    return {"ok": True, "note": "Welcome is active until the bot restarts. Persist this later if needed."}


LOCAL_TOOLS = {
    "local_send_message": local_send_message,
    "local_create_text_channel": local_create_text_channel,
    "local_create_role": local_create_role,
    "local_assign_role": local_assign_role,
    "local_setup_welcome": local_setup_welcome,
}


agent_bot = OwnerAgentBot()
agent_bot.welcome_channel_id = None
agent_bot.welcome_message = None


@agent_bot.event
async def on_member_join(member: discord.Member):
    channel_id = getattr(agent_bot, "welcome_channel_id", None)
    template = getattr(agent_bot, "welcome_message", None)
    if not channel_id or not template:
        return
    channel = agent_bot.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        return
    content = template.replace("{member}", member.mention).replace("{server}", member.guild.name)
    await channel.send(content)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Missing DISCORD_TOKEN in .env")
    agent_bot.run(DISCORD_TOKEN)
