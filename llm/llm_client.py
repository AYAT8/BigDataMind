"""Provider-neutral HTTP adapters for Ollama and OpenAI-compatible APIs."""

from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from llm.settings import LLMSettings, load_settings

LOGGER = logging.getLogger(__name__)
PROFILE_PATH = Path(__file__).with_name("model_profiles.json")
MAX_ATTEMPTS = 3


class LLMError(RuntimeError):
    """Base exception for client and response failures."""


class LLMConnectionError(LLMError):
    """The provider cannot be reached."""


class LLMTimeoutError(LLMError):
    """The provider did not respond within the configured timeout."""


class LLMAuthError(LLMError):
    """The provider rejected the configured credentials."""


class LLMResponseError(LLMError):
    """The provider returned an unsuccessful or malformed response."""


@dataclass(frozen=True)
class LLMResult:
    text: str
    provider: str
    model: str
    latency_s: float
    prompt_tokens: int | None
    completion_tokens: int | None
    finish_reason: str | None


@dataclass(frozen=True)
class ModelProfile:
    name: str
    provider: str
    model: str
    base_url: str
    api_key: str | None = field(default=None, repr=False)


class LLMClient(ABC):
    """Shared generation contract implemented by HTTP provider adapters."""

    provider: str

    def __init__(
        self,
        model: str,
        base_url: str,
        timeout_s: float,
        retry_count: int,
        default_temperature: float,
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.max_attempts = min(retry_count + 1, MAX_ATTEMPTS)
        self.default_temperature = default_temperature

    @abstractmethod
    def _request(
        self, system: str, prompt: str, temperature: float, max_tokens: int
    ) -> tuple[str, int | None, int | None, str | None]:
        """Perform the provider-specific request and parse its response."""

    def generate(
        self,
        system: str,
        prompt: str,
        temperature: float | None = 0.0,
        max_tokens: int = 512,
    ) -> LLMResult:
        effective_temperature = (
            self.default_temperature if temperature is None else temperature
        )
        if not 0 <= effective_temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        if max_tokens <= 0:
            raise ValueError("max_tokens must be a positive integer")
        started = time.perf_counter()
        text, prompt_tokens, completion_tokens, finish_reason = self._request(
            system,
            prompt,
            effective_temperature,
            max_tokens,
        )
        latency = time.perf_counter() - started
        LOGGER.info(
            "LLM generation provider=%s model=%s latency_s=%.3f "
            "prompt_tokens=%s completion_tokens=%s",
            self.provider,
            self.model,
            latency,
            prompt_tokens,
            completion_tokens,
        )
        return LLMResult(
            text=text,
            provider=self.provider,
            model=self.model,
            latency_s=latency,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            finish_reason=finish_reason,
        )

    def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        for attempt in range(self.max_attempts):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_s,
                )
            except requests.Timeout:
                if attempt + 1 < self.max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise LLMTimeoutError(
                    f"{self.provider} request timed out after {self.max_attempts} attempts"
                ) from None
            except requests.ConnectionError:
                if attempt + 1 < self.max_attempts:
                    time.sleep(2**attempt)
                    continue
                message = (
                    f"Ollama is not reachable at {self.base_url}: start it with "
                    "ollama serve"
                    if self.provider == "ollama"
                    else f"{self.provider} is not reachable at the configured endpoint"
                )
                raise LLMConnectionError(message) from None
            except (requests.InvalidURL, requests.InvalidSchema, requests.MissingSchema):
                raise LLMConnectionError(
                    f"{self.provider} endpoint URL is invalid"
                ) from None

            if response.status_code in (401, 403):
                raise LLMAuthError(
                    f"{self.provider} authentication failed (HTTP {response.status_code})"
                )
            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 < self.max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise LLMResponseError(
                    f"{self.provider} returned HTTP {response.status_code} "
                    f"after {self.max_attempts} attempts"
                )
            if not response.ok:
                raise LLMResponseError(
                    f"{self.provider} returned HTTP {response.status_code}"
                )
            try:
                result = response.json()
            except (requests.JSONDecodeError, json.JSONDecodeError, ValueError):
                raise LLMResponseError(
                    f"{self.provider} returned malformed JSON"
                ) from None
            if not isinstance(result, dict):
                raise LLMResponseError(
                    f"{self.provider} returned a malformed response object"
                )
            return result
        raise AssertionError("request attempts exhausted unexpectedly")


class OllamaClient(LLMClient):
    """Adapter for Ollama's native ``/api/chat`` endpoint."""

    provider = "ollama"

    def __init__(self, profile: ModelProfile, settings: LLMSettings) -> None:
        super().__init__(
            profile.model,
            profile.base_url,
            settings.timeout_local_s,
            settings.retry_count,
            settings.default_temperature,
        )
        self.num_ctx = settings.ollama_num_ctx
        self.keep_alive = settings.ollama_keep_alive

    def _request(
        self, system: str, prompt: str, temperature: float, max_tokens: int
    ) -> tuple[str, int | None, int | None, str | None]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_ctx": self.num_ctx,
                "num_predict": max_tokens,
            },
            "keep_alive": self.keep_alive,
        }
        result = self._post_json(f"{self.base_url}/api/chat", payload)
        message = result.get("message")
        if not isinstance(message, dict) or not isinstance(message.get("content"), str):
            raise LLMResponseError("Ollama returned a malformed chat response")
        return (
            message["content"],
            _optional_count(result.get("prompt_eval_count"), "prompt_eval_count"),
            _optional_count(result.get("eval_count"), "eval_count"),
            _optional_string(result.get("done_reason"), "done_reason"),
        )


class OpenAICompatibleClient(LLMClient):
    """Adapter for OpenAI-compatible ``/chat/completions`` endpoints."""

    provider = "openai_compatible"

    def __init__(self, profile: ModelProfile, settings: LLMSettings) -> None:
        if not profile.api_key:
            raise ValueError("Hosted profile requires BDM_HOSTED_API_KEY")
        super().__init__(
            profile.model,
            profile.base_url,
            settings.timeout_hosted_s,
            settings.retry_count,
            settings.default_temperature,
        )
        self._api_key = profile.api_key

    def _request(
        self, system: str, prompt: str, temperature: float, max_tokens: int
    ) -> tuple[str, int | None, int | None, str | None]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        result = self._post_json(
            f"{self.base_url}/chat/completions",
            payload,
            headers={"Authorization": f"Bearer {self._api_key}"},
        )
        choices = result.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise LLMResponseError(
                "OpenAI-compatible provider returned a malformed chat response"
            )
        message = choices[0].get("message")
        if not isinstance(message, dict) or not isinstance(message.get("content"), str):
            raise LLMResponseError(
                "OpenAI-compatible provider returned a malformed chat response"
            )
        usage = result.get("usage")
        if usage is not None and not isinstance(usage, dict):
            raise LLMResponseError(
                "OpenAI-compatible provider returned malformed token usage"
            )
        usage = usage or {}
        return (
            message["content"],
            _optional_count(usage.get("prompt_tokens"), "prompt_tokens"),
            _optional_count(usage.get("completion_tokens"), "completion_tokens"),
            _optional_string(choices[0].get("finish_reason"), "finish_reason"),
        )


def _optional_count(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LLMResponseError(f"Provider returned invalid {field_name}")
    return value


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise LLMResponseError(f"Provider returned invalid {field_name}")
    return value


def load_profiles() -> dict[str, dict[str, str]]:
    """Read the secret-free provider profile definitions."""
    try:
        profiles = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Unable to load llm/model_profiles.json") from exc
    if not isinstance(profiles, list):
        raise ValueError("Model profiles must be a JSON list")
    by_name: dict[str, dict[str, str]] = {}
    for profile in profiles:
        if not isinstance(profile, dict) or not isinstance(profile.get("name"), str):
            raise ValueError("Each model profile must have a name")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in profile.items()
        ):
            raise ValueError("Model profile keys and values must be strings")
        if profile["name"] in by_name:
            raise ValueError(f"Duplicate model profile: {profile['name']}")
        required = (
            ("model", "base_url")
            if profile.get("provider") == "ollama"
            else ("model_env", "base_url_env", "api_key_env")
            if profile.get("provider") == "openai_compatible"
            else None
        )
        if required is None:
            raise ValueError(f"Unsupported provider in profile {profile['name']}")
        missing = [key for key in required if not profile.get(key)]
        if missing:
            raise ValueError(
                f"Profile {profile['name']} is missing: {', '.join(missing)}"
            )
        by_name[profile["name"]] = profile
    return by_name


def create_client(
    profile: str | ModelProfile, settings: LLMSettings | None = None
) -> LLMClient:
    """Instantiate a client from a named JSON profile or a resolved profile."""
    settings = settings or load_settings()
    if isinstance(profile, str):
        try:
            definition = load_profiles()[profile]
        except KeyError as exc:
            raise ValueError(f"Unknown model profile: {profile}") from exc
        provider = definition.get("provider")
        if provider == "ollama":
            model_profile = ModelProfile(
                name=profile,
                provider=provider,
                model=definition["model"],
                base_url=definition["base_url"],
            )
        elif provider == "openai_compatible":
            base_url_name = definition["base_url_env"]
            model_name = definition["model_env"]
            api_key_name = definition["api_key_env"]
            configured_values = {
                "BDM_HOSTED_BASE_URL": settings.hosted_base_url,
                "BDM_HOSTED_MODEL": settings.hosted_model,
                "BDM_HOSTED_API_KEY": settings.hosted_api_key,
            }
            base_url = configured_values.get(base_url_name) or os.getenv(base_url_name)
            model = configured_values.get(model_name) or os.getenv(model_name)
            api_key = configured_values.get(api_key_name) or os.getenv(api_key_name)
            if not base_url or not model or not api_key:
                missing = [
                    variable
                    for variable, value in (
                        (base_url_name, base_url),
                        (model_name, model),
                        (api_key_name, api_key),
                    )
                    if not value
                ]
                raise ValueError(
                    "Hosted profile requires " + ", ".join(missing)
                )
            model_profile = ModelProfile(
                name=profile,
                provider=provider,
                model=model,
                base_url=base_url,
                api_key=api_key,
            )
        else:
            raise ValueError(f"Unsupported provider in profile {profile}")
    else:
        model_profile = profile

    if model_profile.provider == "ollama":
        return OllamaClient(model_profile, settings)
    if model_profile.provider == "openai_compatible":
        return OpenAICompatibleClient(model_profile, settings)
    raise ValueError(f"Unsupported provider: {model_profile.provider}")
