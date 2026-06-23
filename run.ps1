$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (!(Test-Path ".venv")) {
    python -m venv .venv
}

.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

if (!(Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env. Edit it with your DISCORD_TOKEN, then run this again."
    exit 1
}

if (!(Test-Path "config.json")) {
    Copy-Item "config.example.json" "config.json"
}

python -u bot.py
