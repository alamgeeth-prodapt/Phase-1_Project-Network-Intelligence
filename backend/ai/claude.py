import os
from pathlib import Path

from anthropic import Anthropic
from dotenv import load_dotenv


# Explicitly load backend/.env.backend
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BASE_DIR / ".env.backend"

load_dotenv(ENV_FILE)

API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not API_KEY:
    raise RuntimeError(
        f"ANTHROPIC_API_KEY is not configured. "
        f"Checked: {ENV_FILE}"
    )

client = Anthropic(api_key=API_KEY)

MODEL = "claude-sonnet-5"