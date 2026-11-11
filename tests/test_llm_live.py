"""Explicitly opt-in local LLM smoke test."""

import os

import pytest

from llm.llm_client import create_client


@pytest.mark.live
@pytest.mark.skipif(
    os.getenv("BDM_RUN_LIVE_LLM") != "1",
    reason="set BDM_RUN_LIVE_LLM=1 to run live provider checks",
)
def test_local_ollama_generation():
    result = create_client("local-3b").generate(
        "Reply with exactly OK.",
        "OK",
        temperature=0.0,
        max_tokens=8,
    )
    assert result.text
