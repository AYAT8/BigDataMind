"""Environment-backed settings for provider-neutral LLM clients."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LLMSettings:
    timeout_local_s: float = 120.0
    timeout_hosted_s: float = 60.0
    retry_count: int = 2
    default_temperature: float = 0.0
    ollama_num_ctx: int = 4096
    ollama_keep_alive: str = "5m"
    hosted_base_url: str | None = None
    hosted_model: str | None = None
    hosted_api_key: str | None = field(default=None, repr=False)


def load_settings(environ: Mapping[str, str] | None = None) -> LLMSettings:
    """Load validated settings; secrets are kept out of the dataclass repr."""
    if environ is None:
        load_dotenv(PROJECT_ROOT / ".env")
        environ = os.environ

    def positive_float(name: str, default: float) -> float:
        try:
            value = float(environ.get(name, default))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a positive number") from exc
        if value <= 0:
            raise ValueError(f"{name} must be a positive number")
        return value

    def positive_int(name: str, default: int) -> int:
        try:
            value = int(environ.get(name, default))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a positive integer") from exc
        if value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    try:
        retry_count = int(environ.get("BDM_LLM_RETRY_COUNT", "2"))
    except (TypeError, ValueError) as exc:
        raise ValueError("BDM_LLM_RETRY_COUNT must be between 0 and 2") from exc
    if not 0 <= retry_count <= 2:
        raise ValueError("BDM_LLM_RETRY_COUNT must be between 0 and 2")

    try:
        temperature = float(environ.get("BDM_LLM_TEMPERATURE", "0"))
    except (TypeError, ValueError) as exc:
        raise ValueError("BDM_LLM_TEMPERATURE must be between 0 and 2") from exc
    if not 0 <= temperature <= 2:
        raise ValueError("BDM_LLM_TEMPERATURE must be between 0 and 2")

    return LLMSettings(
        timeout_local_s=positive_float("BDM_LLM_TIMEOUT_LOCAL", 120.0),
        timeout_hosted_s=positive_float("BDM_LLM_TIMEOUT_HOSTED", 60.0),
        retry_count=retry_count,
        default_temperature=temperature,
        ollama_num_ctx=positive_int("BDM_OLLAMA_NUM_CTX", 4096),
        ollama_keep_alive=environ.get("BDM_OLLAMA_KEEP_ALIVE", "5m"),
        hosted_base_url=environ.get("BDM_HOSTED_BASE_URL") or None,
        hosted_model=environ.get("BDM_HOSTED_MODEL") or None,
        hosted_api_key=environ.get("BDM_HOSTED_API_KEY") or None,
    )
