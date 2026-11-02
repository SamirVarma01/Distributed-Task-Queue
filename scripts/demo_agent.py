"""Run an agent end to end on a running stack, printing each step as it finishes.

    export ANTHROPIC_API_KEY=...            # and/or OPENAI_API_KEY, before starting the stack
    docker compose up -d --build
    python scripts/demo_agent.py "What is 1234 * 5678? Send the answer to ops."
    python scripts/demo_agent.py --provider openai --model <model> "..."
    python scripts/demo_agent.py --provider scripted --model demo-approval "anything"  # no API key needed

When the run waits for approval, the script asks you, unless --approve or --reject
answers every approval for you. Uses only the standard library."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

DEFAULT_TOOLS = "calculator,http_get,send_notification,publish_announcement"
DEFAULT_MODELS = {"anthropic": "claude-opus-5-5"}
FINISHED = {"succeeded", "failed", "unknown_outcome", "waiting"}
TERMINAL = {"succeeded", "failed", "cancelled", "budget_exceeded"}


def call(api: str, method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(api + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        hint = ("\nA provider is enabled when its API key (ANTHROPIC_API_KEY, OPENAI_API_KEY) is set in the "
                "shell that starts the stack: export it, then run docker compose up -d.") if "unknown provider" in detail else ""
        sys.exit(f"{method} {path} -> {exc.code}: {detail}{hint}")


def short(value, limit: int = 100) -> str:
    text = value if isinstance(value, str) else json.dumps(value)
    return text if len(text) <= limit else text[:limit - 3] + "..."


def describe(step: dict) -> str:
    out, inp = step["output"], step["input"]
    if step["kind"] == "llm":
        if step["status"] != "succeeded":
            return f"failed: {out['error']}"
        usage = f"  ({step['input_tokens']} in / {step['output_tokens']} out tokens)"
        calls = ", ".join(f"{c['name']}({short(c['args'], 60)})" for c in out["tool_calls"])
        return (f"calls {calls}" if calls else f"answers: {short(out['text'], 200)}") + usage
    if step["kind"] == "tool":
        call_text = f"{inp['tool']}({short(inp['args'], 60)})"
        if step["status"] == "succeeded":
            return f"{call_text} -> {short(out)}"
        return f"{call_text} {step['status']}: {short(out.get('error', out) if isinstance(out, dict) else out)}"
    if step["status"] == "waiting":
        calls = ", ".join(f"{c['tool']}({short(c['args'], 60)})" for c in inp["calls"])
        return f"waiting for approval ({inp['reason']}): {calls}"
    return f"decided: {out['decision']}" + (f" ({out['note']})" if out.get("note") else "")


def decide(api: str, run_id: str, step: dict, answer: str | None) -> None:
    if answer is None:
        prompt = ("Retry the call? " if step["input"]["reason"] == "unknown_outcome" else "Approve? ")
        try:
            answer = "approve" if input(prompt + "[y/N] ").strip().lower() in ("y", "yes") else "reject"
        except EOFError:  # no terminal to ask on
            print("(no answer)")
            answer = "reject"
    call(api, "POST", f"/runs/{run_id}/{answer}", {"step_no": step["step_no"], "note": "from demo_agent.py"})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("goal")
    parser.add_argument("--provider", default="anthropic")
    parser.add_argument("--model", help="default: claude-opus-5-5 for anthropic; required for other providers")
    parser.add_argument("--tools", default=DEFAULT_TOOLS, help="comma-separated tool names")
    parser.add_argument("--system", default="")
    parser.add_argument("--effort", help="e.g. low, medium, high")
    parser.add_argument("--max-steps", type=int, default=10, help="LLM calls")
    parser.add_argument("--max-cost-micros", type=int)
    parser.add_argument("--api", default="http://localhost:8000")
    answer = parser.add_mutually_exclusive_group()
    answer.add_argument("--approve", dest="answer", action="store_const", const="approve")
    answer.add_argument("--reject", dest="answer", action="store_const", const="reject")
    args = parser.parse_args()

    model = args.model or DEFAULT_MODELS.get(args.provider)
    if not model:
        parser.error(f"--model is required for provider {args.provider!r}")
    model_config = {"provider": args.provider, "name": model, "system": args.system}
    if args.effort:
        model_config["effort"] = args.effort
    body = {"goal": args.goal, "model": model_config, "max_steps": args.max_steps,
            "tools": [t for t in args.tools.split(",") if t]}
    if args.max_cost_micros:
        body["max_cost_micros"] = args.max_cost_micros
    run = call(args.api, "POST", "/runs", body)
    print(f"run {run['id']}  {args.provider}/{model}  tools: {', '.join(body['tools'])}")

    shown: set[tuple[int, str]] = set()
    while True:
        run = call(args.api, "GET", f"/runs/{run['id']}")
        for step in call(args.api, "GET", f"/runs/{run['id']}/steps"):
            if step["status"] in FINISHED and (step["step_no"], step["status"]) not in shown:
                shown.add((step["step_no"], step["status"]))
                print(f"  [{step['step_no']:>2}] {step['kind']:<8} {describe(step)}")
                if step["status"] == "waiting" and run["status"] == "awaiting_approval":
                    decide(args.api, run["id"], step, args.answer)
        if run["status"] in TERMINAL:
            break
        time.sleep(0.5)

    print(f"\n{run['status']}: {run['result'] if run['status'] == 'succeeded' else run['error']}")
    print(f"{run['llm_calls']} LLM calls, {run['input_tokens']} input and {run['output_tokens']} output tokens, "
          f"${run['cost_micros'] / 1_000_000:.4f}" + ("" if run["cost_micros"] else " (no price for this model)"))


if __name__ == "__main__":
    main()
