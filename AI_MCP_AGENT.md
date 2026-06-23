# Owner-Only AI MCP Discord Agent

This is the advanced bot architecture you described.

Important: the recommended version is now merged into `bot.py`, so `run.ps1` starts the bot with both button features and owner-only AI MCP control.

You talk naturally to the Discord bot. The bot sends your request to the configured AI API. The AI chooses Discord MCP tools or local Discord tools. The bot executes those tools and replies only to the owner.

## What This Bot Does

- Listens only to users in `OWNER_IDS`
- Works in DM, when mentioned, or when a message starts with `AI_AGENT_PREFIX`
- Sends your natural language request to the AI model
- Gives the AI a list of available Discord MCP tools
- Lets the AI return JSON tool calls
- Executes those MCP tools
- Reports the result back to you
- Includes local fallback tools for common tasks:
  - Send message
  - Create text channel
  - Create role
  - Assign role
  - Configure simple welcome message

## Required `.env`

```env
DISCORD_TOKEN=your_discord_bot_token
GUILD_ID=1518702371428106290

AI_API_KEY=your_nvidia_or_openai_key
AI_BASE_URL=https://integrate.api.nvidia.com/v1
AI_MODEL=minimaxai/minimax-m3

OWNER_IDS=1439166297496621117

DISCORD_MCP_URL=your_discord_mcp_http_or_sse_endpoint
MCP_GUILD_ID=1518702371428106290
AI_AGENT_PREFIX=!
```

`DISCORD_MCP_URL` must be the HTTP/SSE endpoint of your MCP server. If your MCP server is stdio-only, you need to run it behind an MCP HTTP bridge or tell Codex the exact command you use to start it so the transport adapter can be changed.

## Run The Merged Bot

```powershell
cd C:\Users\shaik\Documents\Codex\2026-06-23\plugin-creator-c-users-shaik-codex-2\outputs\manager-bot
.\run.ps1
```

## How To Talk To It

DM the bot:

```text
list all channels
```

Or mention it in your server:

```text
@YourBot create a category called Support and a ticket channel inside it
```

Or use the prefix:

```text
!create a role called VIP Customer and give it to user 123456789
```

## Safety

The AI prompt tells the bot to ask for confirmation before destructive actions such as deleting categories, deleting channels, banning, kicking, or mass changes.

The bot ignores everyone except IDs in `OWNER_IDS`.

## Important

`ai_mcp_bot.py` remains in the folder as a smaller standalone experiment. Use `bot.py` for the full merged version.
