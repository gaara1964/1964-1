$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (!(Test-Path ".venv")) {
    python -m venv .venv
}

.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

if (!(Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env. Add DISCORD_TOKEN, AI_API_KEY, and DISCORD_MCP_URL, then run this again."
    exit 1
}

python ai_mcp_bot.py
