# backend/gpt_client.py

import json
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from backend.config import OPENAI_API_KEY, OPENAI_MODEL, OPENAI_BASE_URL, GPT_ENABLED


@dataclass
class GPTClient:
    model: str = OPENAI_MODEL
    base_url: str = OPENAI_BASE_URL
    api_key: str = OPENAI_API_KEY

    def is_enabled(self) -> bool:
        return GPT_ENABLED and bool((self.api_key or "").strip())

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def classify_primary_category_case1(
        self,
        name: str,
        raw_category: str = "",
        address: str = "",
        allowed: Optional[List[str]] = None,
    ) -> str:
        # Case 1 ONLY
        # Returns empty string if GPT disabled or error occurs

        if not self.is_enabled():
            return ""

        if allowed is None:
            allowed = [
                "Manufacturing",
                "Industrial Supplier",
                "Factory",
                "Packaging",
                "Engineering",
                "Automotive",
                "Food Processing",
                "Chemical",
                "Textile",
                "Electrical",
                "Construction Materials",
                "Other",
            ]

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a business categorization assistant. "
                        "Return ONLY JSON like {\"primary_category\": \"...\"}. "
                        "The value must be from the allowed list."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "business": {
                                "name": name,
                                "raw_category": raw_category,
                                "address": address,
                            },
                            "allowed_primary_categories": allowed,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            "temperature": 0.2,
        }

        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self._headers(),
                json=payload,
                timeout=25,
            )
            response.raise_for_status()

            content = response.json()["choices"][0]["message"]["content"]
            obj = json.loads(content)
            category = (obj.get("primary_category") or "").strip()

            return category if category in allowed else "Other"

        except Exception:
            return ""
