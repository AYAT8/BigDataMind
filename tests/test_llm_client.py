"""Offline tests for provider adapters using a standard-library HTTP stub."""

from __future__ import annotations

import csv
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from llm.llm_client import (
    LLMAuthError,
    LLMConnectionError,
    LLMResult,
    LLMResponseError,
    LLMTimeoutError,
    ModelProfile,
    OllamaClient,
    OpenAICompatibleClient,
    create_client,
    load_profiles,
)
from llm.settings import LLMSettings, load_settings
from llm import compare


class StubState:
    statuses: list[int] = []
    calls: list[dict[str, object]] = []
    delay_s = 0.0
    malformed = False


@pytest.fixture
def stub_server():
    state = StubState()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            content_length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(content_length))
            state.calls.append(
                {
                    "path": self.path,
                    "body": body,
                    "authorization": self.headers.get("Authorization"),
                }
            )
            if state.delay_s:
                time.sleep(state.delay_s)
            status = state.statuses.pop(0) if state.statuses else 200
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if state.malformed:
                response_body = b"{invalid json"
            elif self.path.endswith("/api/chat"):
                response_body = json.dumps(
                    {
                        "message": {"content": "SELECT 1"},
                        "prompt_eval_count": 12,
                        "eval_count": 3,
                        "done_reason": "stop",
                    }
                ).encode()
            else:
                response_body = json.dumps(
                    {
                        "choices": [
                            {
                                "message": {"content": "SELECT 1"},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 12, "completion_tokens": 3},
                    }
                ).encode()
            try:
                self.wfile.write(response_body)
            except BrokenPipeError:
                pass

        def log_message(self, _format, *_args):
            return

    class Server(ThreadingHTTPServer):
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.statuses = []
    state.calls = []
    state.delay_s = 0.0
    state.malformed = False
    yield f"http://127.0.0.1:{server.server_port}", state
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _settings(**overrides):
    values = {
        "timeout_local_s": 1.0,
        "timeout_hosted_s": 1.0,
        "retry_count": 2,
        "default_temperature": 0.0,
        "ollama_num_ctx": 4096,
        "ollama_keep_alive": "5m",
    }
    values.update(overrides)
    return LLMSettings(**values)


def _ollama(base_url, settings=None):
    return OllamaClient(
        ModelProfile(
            name="test-local",
            provider="ollama",
            model="test-model",
            base_url=base_url,
        ),
        settings or _settings(),
    )


def _hosted(base_url, settings=None, api_key="test-secret"):
    return OpenAICompatibleClient(
        ModelProfile(
            name="test-hosted",
            provider="openai_compatible",
            model="test-model",
            base_url=base_url,
            api_key=api_key,
        ),
        settings or _settings(),
    )


def test_ollama_success_parses_tokens_and_request_options(stub_server):
    base_url, state = stub_server
    result = _ollama(
        base_url,
        _settings(ollama_num_ctx=8192, ollama_keep_alive="10m"),
    ).generate("system", "question", temperature=0.25, max_tokens=64)

    assert result.text == "SELECT 1"
    assert result.provider == "ollama"
    assert result.prompt_tokens == 12
    assert result.completion_tokens == 3
    assert result.finish_reason == "stop"
    body = state.calls[0]["body"]
    assert body["options"] == {
        "temperature": 0.25,
        "num_ctx": 8192,
        "num_predict": 64,
    }
    assert body["keep_alive"] == "10m"


def test_openai_compatible_success_parses_tokens(stub_server):
    base_url, state = stub_server
    result = _hosted(base_url).generate("system", "question")
    assert result.text == "SELECT 1"
    assert result.provider == "openai_compatible"
    assert result.prompt_tokens == 12
    assert result.completion_tokens == 3
    assert result.finish_reason == "stop"
    assert state.calls[0]["authorization"] == "Bearer test-secret"


def test_configured_temperature_is_used_when_not_overridden(stub_server):
    base_url, state = stub_server
    _hosted(base_url, _settings(default_temperature=0.4)).generate(
        "system",
        "question",
        temperature=None,
    )
    assert state.calls[0]["body"]["temperature"] == 0.4


def test_retry_after_server_error_succeeds(stub_server, monkeypatch):
    base_url, state = stub_server
    state.statuses = [500, 200]
    monkeypatch.setattr("llm.llm_client.time.sleep", lambda _seconds: None)
    result = _hosted(base_url).generate("system", "question")
    assert result.text == "SELECT 1"
    assert len(state.calls) == 2


def test_retry_after_rate_limit_succeeds(stub_server, monkeypatch):
    base_url, state = stub_server
    state.statuses = [429, 200]
    monkeypatch.setattr("llm.llm_client.time.sleep", lambda _seconds: None)
    _hosted(base_url).generate("system", "question")
    assert len(state.calls) == 2


def test_auth_failure_is_not_retried_and_secret_is_not_exposed(
    stub_server, caplog
):
    base_url, state = stub_server
    state.statuses = [401]
    secret = "sensitive-test-key"
    caplog.set_level(logging.INFO)
    with pytest.raises(LLMAuthError) as exc_info:
        _hosted(base_url, api_key=secret).generate("system", "question")
    assert len(state.calls) == 1
    assert secret not in str(exc_info.value)
    assert secret not in caplog.text


def test_malformed_json_is_not_retried(stub_server):
    base_url, state = stub_server
    state.malformed = True
    with pytest.raises(LLMResponseError, match="malformed JSON"):
        _hosted(base_url).generate("system", "question")
    assert len(state.calls) == 1


def test_timeout_becomes_timeout_error(stub_server):
    base_url, state = stub_server
    state.delay_s = 0.2
    client = _ollama(base_url, _settings(timeout_local_s=0.05, retry_count=0))
    with pytest.raises(LLMTimeoutError):
        client.generate("system", "question")


def test_unreachable_ollama_has_helpful_message():
    client = _ollama("http://127.0.0.1:1", _settings(retry_count=0))
    with pytest.raises(LLMConnectionError, match="start it with ollama serve"):
        client.generate("system", "question")


def test_settings_load_environment_and_profile_json():
    settings = load_settings(
        {
            "BDM_LLM_TIMEOUT_LOCAL": "90",
            "BDM_LLM_TIMEOUT_HOSTED": "45",
            "BDM_LLM_RETRY_COUNT": "1",
            "BDM_LLM_TEMPERATURE": "0.2",
            "BDM_OLLAMA_NUM_CTX": "8192",
            "BDM_OLLAMA_KEEP_ALIVE": "20m",
            "BDM_HOSTED_BASE_URL": "https://example.test/v1",
            "BDM_HOSTED_MODEL": "model",
            "BDM_HOSTED_API_KEY": "hidden-key",
        }
    )
    assert settings.timeout_local_s == 90
    assert settings.timeout_hosted_s == 45
    assert settings.retry_count == 1
    assert settings.default_temperature == 0.2
    assert settings.ollama_num_ctx == 8192
    assert "hidden-key" not in repr(settings)
    profiles = load_profiles()
    assert profiles["local-3b"]["model"] == "qwen2.5-coder:3b"
    assert profiles["hosted"]["api_key_env"] == "BDM_HOSTED_API_KEY"
    client = create_client("hosted", settings)
    assert isinstance(client, OpenAICompatibleClient)


def test_hosted_profile_without_configuration_fails_clearly():
    with pytest.raises(ValueError, match="BDM_HOSTED_BASE_URL"):
        create_client(
            "hosted",
            _settings(),
        )


def test_comparison_records_unavailable_profiles_without_stopping(
    monkeypatch, tmp_path
):
    class WorkingClient:
        def generate(self, *_args, **_kwargs):
            return LLMResult(
                text="SELECT 1\nFROM gold_daily_demand",
                provider="ollama",
                model="test-model",
                latency_s=0.1,
                prompt_tokens=10,
                completion_tokens=5,
                finish_reason="stop",
            )

    def fake_create_client(profile, _settings):
        if profile == "missing":
            raise ValueError("provider configuration is missing")
        return WorkingClient()

    monkeypatch.setattr(compare, "create_client", fake_create_client)
    markdown_path, csv_path = compare.run_comparison(
        ["missing", "working"], output_dir=tmp_path
    )
    with csv_path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))
    assert len(rows) == 20
    assert sum(row["error"].startswith("skipped:") for row in rows) == 10
    assert (
        sum(
            row["raw_output"] == "SELECT 1\nFROM gold_daily_demand"
            for row in rows
        )
        == 10
    )
    assert all(row["manual_grade"] == "" for row in rows)
    assert "<br>" in markdown_path.read_text(encoding="utf-8")
