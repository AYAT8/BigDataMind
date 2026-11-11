# LLM setup

Milestone 3.1 provides two HTTP adapters behind one `LLMClient` interface:
Ollama's native chat API for local models and an OpenAI-compatible chat
completion endpoint for hosted providers. The adapters record generated text,
provider/model, latency, token usage, and finish reason. They do not execute or
validate generated SQL.

## Local Ollama

Install Ollama and pull the model yourself; this project does not install
Ollama or download model files. To keep model files on the D: drive in WSL,
configure Ollama's model directory before starting the service:

```bash
export OLLAMA_MODELS=/mnt/d/ollama-models
ollama serve
```

In another Ubuntu terminal, pull the configured 3B model:

```bash
ollama pull qwen2.5-coder:3b
```

The client uses `http://localhost:11434`, explicitly sets `num_ctx` to avoid
Ollama silently truncating prompts at its smaller implicit context window, and
keeps the model loaded for five minutes by default.

## Hosted provider

Copy `.env.example` to `.env` and set `BDM_HOSTED_BASE_URL`,
`BDM_HOSTED_MODEL`, and `BDM_HOSTED_API_KEY` to values from your
OpenAI-compatible provider. `.env` is ignored by Git; do not put credentials in
tracked files, command output, or benchmark notes.

```bash
cp .env.example .env
```

Timeouts default to 120 seconds for local CPU models and 60 seconds for hosted
requests. Up to two retries (three attempts total) with exponential backoff are
made for connection errors, timeouts, HTTP 429, and HTTP 5xx only. HTTP 401/403
and malformed responses fail without retrying. Temperature defaults to zero
for repeatable SQL-generation prompts.

## Smoke comparison

With the desired provider(s) running/configured, run from the repository root:

```bash
python -m llm.compare --profiles local-3b hosted
```

The command records ten fixed English and French questions, raw outputs,
latency, available token counts, errors, and an empty manual-grade field in
timestamped Markdown and CSV files under `docs/benchmarks/`. The comparison
does not grade SQL; semantic and safety validation belong to milestone 3.2.
Unavailable profiles are reported as skipped without preventing other profiles
from running. Benchmark outputs are local and ignored by Git.

Network-free client tests run with `python -m pytest -q`. Live Ollama tests are
skipped by default; opt in with `BDM_RUN_LIVE_LLM=1`.