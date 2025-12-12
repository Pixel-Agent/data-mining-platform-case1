# backend/config.py
import os

# Case 1 only
DEFAULT_LOCATION = os.getenv("CASE1_DEFAULT_LOCATION", "India")

# GPT (optional)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

GPT_ENABLED = bool(OPENAI_API_KEY.strip())
