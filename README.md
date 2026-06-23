# Manager Bot

One powerful custom Discord bot for a gaming and product-selling server.

It is designed to replace most external bots by handling:

- Ticket panels with buttons
- Private ticket channels
- Claim, waiting, resolved, and close ticket buttons
- Ticket transcripts and logs
- Role verification and role select menus
- Giveaways with join buttons and automatic winners
- Order tracking
- Verified customer role assignment
- Warnings, timeouts, purge, and moderation logs
- Anti-spam, anti-invite, mass mention detection, and blocked words
- Server setup, panels, staff channels, and backups
- Optional AI ticket replies and staff summaries

## Merged Bot Mode

`bot.py` is now the merged bot:

- Built-in ticket buttons, role menus, giveaways, orders, moderation, welcome messages, and setup commands
- Owner-only AI control through DM, mention, or the `AI_AGENT_PREFIX`
- AI API planning through your configured model
- Discord MCP tool execution through `DISCORD_MCP_URL`
- Local fallback tools for creating channels, roles, assigning roles, posting panels, and setup
- Normal users can mention or DM the bot to ask questions from configured memory, instruction, and knowledge channels

`ai_mcp_bot.py` is kept as a separate experimental legacy runner, but the recommended bot is now `bot.py` through `run.ps1`.

## 1. Create The Discord Bot

1. Go to the Discord Developer Portal.
2. Create an application.
3. Open **Bot** and create/reset the bot token.
4. Enable these privileged gateway intents:
   - Server Members Intent
   - Message Content Intent
5. Invite the bot with these permissions:
   - Administrator, easiest for first setup
   - Or manually: Manage Channels, Manage Roles, Manage Messages, Read Message History, Send Messages, Embed Links, Attach Files, Use Slash Commands, Moderate Members, View Channels

Important: The bot role must be above any role it needs to assign, including `Member`, `Verified Customer`, and self-service roles.

## 2. Install

Open PowerShell and run:

```powershell
cd C:\Users\shaik\Documents\Codex\2026-06-23\plugin-creator-c-users-shaik-codex-2\outputs\manager-bot
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
Copy-Item config.example.json config.json
```

Edit `.env`:

```env
DISCORD_TOKEN=your_bot_token
GUILD_ID=your_server_id
AI_API_KEY=
AI_BASE_URL=https://api.openai.com/v1
AI_MODEL=gpt-4.1-mini
OWNER_IDS=your_discord_user_id
DISCORD_MCP_URL=your_discord_mcp_http_or_sse_endpoint
MCP_GUILD_ID=your_server_id
AI_AGENT_PREFIX=!
```

`AI_API_KEY` is optional. Without it, all normal bot features work, but AI ticket replies and AI summaries are disabled.

For NVIDIA NIM / build.nvidia.com, use:

```env
AI_API_KEY=your_nvapi_key
AI_BASE_URL=https://integrate.api.nvidia.com/v1
AI_MODEL=minimaxai/minimax-m3
```

## 3. Run

```powershell
python bot.py
```

Or use the included runner:

```powershell
.\run.ps1
```

When the bot is online, run this slash command in Discord:

```text
/setup_server
```

That creates the main roles/channels and posts:

- Ticket panel
- Role panel
- Staff channels
- Log channels
- AI context channels:
  - `#bot-memory`
  - `#bot-instructions`
  - `#bot-knowledge`

You can also talk to the merged AI agent directly:

```text
!list all channels using MCP
!create a role called VIP Customer and post the role panel in #roles
@YourBot set a welcome message in this channel saying Welcome {member} to {server}
```

Only IDs in `OWNER_IDS` can use the AI control agent.

## AI Knowledge Channels

The bot has three context channels for normal user answers:

- `#bot-instructions`: how the bot should answer, server rules, tone, escalation policy
- `#bot-knowledge`: product info, prices, delivery rules, support FAQ, server info
- `#bot-memory`: useful past Q&A and remembered facts

Run `/setup_server` to create and auto-register them, or use:

```text
/ai_context_channels memory:#bot-memory instructions:#bot-instructions knowledge:#bot-knowledge
```

Normal users cannot call MCP tools. They can only DM the bot or mention it in a channel and ask questions. Owner commands are still controlled by `OWNER_IDS` and `AI_AGENT_PREFIX`.

Example normal user question:

```text
@YourBot what products do you sell?
```

Example owner command:

```text
!1964 create a new channel called deals and post a message there
```

## Main Commands

### Setup

- `/setup_server`
- `/ticket_panel`
- `/role_panel`
- `/backup_create`

### Tickets

Users open tickets with buttons.

Inside a ticket, staff can use:

- Claim
- Waiting
- Resolved
- Close
- `/ticket_summary`, if AI is configured
- `/ticket_add_user`
- `/ticket_remove_user`
- `/ticket_rename`

Professional ticket setup:

```text
/ticket_setup panel_channel:#open-a-ticket logs_channel:#ticket-logs ticket_category:TICKETS support_role:@Support
/ticket_option_add key:purchase label:"Purchase Help" description:"Orders, delivery, and product questions" emoji:🛒 priority:high questions:"Which product?|Do you have an order ID?"
/ticket_option_add key:report label:"Report Issue" description:"Report scams, users, or server problems" emoji:🚨 priority:urgent questions:"Who/what are you reporting?|Send proof or details"
/ticket_options
/ticket_panel
```

The ticket panel uses a dropdown, creates private numbered channels like `purchase-help-0001`, pings the configured support role, logs claims/closes, and saves transcripts when closed.

### Giveaways

```text
/giveaway_create prize:"Nitro" duration:"24h" winners:1
/giveaway_reroll giveaway_id:1
```

Durations support:

- `30m`
- `12h`
- `7d`

### Orders

```text
/order_create user:@name product:"Product Name" notes:"Optional note"
/order_status order_id:1 status:paid notes:"Payment received"
/customer_add user:@name order_id:1
```

### Moderation

```text
/warn user:@name reason:"Reason here"
/timeout user:@name minutes:10 reason:"Spam"
/purge amount:20
```

## Customize

Edit `config.json` to change:

- Brand name
- Channel names
- Role names
- Ticket types
- Ticket questions
- FAQ answers
- Role menu groups
- Banned words
- Anti-invite behavior

Restart the bot after editing `config.json`.

## 24/7 Hosting

For real 24/7 uptime, run it on:

- A VPS
- A Windows machine that stays on
- A hosting provider that supports Python bots

Your local PC works for testing, but the bot goes offline when your PC sleeps or the script stops.

## Notes

This bot intentionally does not process payments or make refund promises. For a product-selling server, keep payment/refund decisions staff-controlled. The AI assistant, if enabled, is designed to answer simple questions and escalate sensitive issues.
