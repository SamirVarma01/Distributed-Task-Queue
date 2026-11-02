import io
import urllib.error

import pytest

from dtq.agents import demo, tools
from dtq.agents.tools import ToolError

from .agent_support import drain


@pytest.mark.parametrize("expression,value", [
    ("2 + 3 * 4", 14), ("(1 + 2) ** 3", 27), ("7 // 2", 3), ("-7 % 3", 2), ("1 / 4", 0.25), ("+1.5", 1.5),
])
def test_calculator(expression, value):
    assert demo.calculator(expression) == value


@pytest.mark.parametrize("expression,error", [
    ("1 / 0", "division by zero"),
    ("2 ** 1000", "exponents above 100"),
    ("__import__('os').system('true')", "only numbers"),
    ("x + 1", "only numbers"),
    ("True + 1", "only numbers"),
    ("1 +", "not an arithmetic expression"),
])
def test_calculator_refuses_anything_but_arithmetic(expression, error):
    with pytest.raises(ToolError, match=error):
        demo.calculator(expression)


class FakeResponse(io.BytesIO):
    status = 200


def test_http_get(monkeypatch):
    urls = []

    def urlopen(url, timeout):
        urls.append((url, timeout))
        return FakeResponse(b"hello " * 10)

    monkeypatch.setattr(demo.urllib.request, "urlopen", urlopen)

    assert demo.http_get("https://example.com/", max_chars=11) == {"status": 200, "body": "hello hello"}
    assert urls == [("https://example.com/", demo.HTTP_TIMEOUT_SECONDS)]


def test_http_get_returns_error_statuses_and_refuses_unreachable_urls(monkeypatch):
    def not_found(url, timeout):
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, io.BytesIO(b"no such page"))

    monkeypatch.setattr(demo.urllib.request, "urlopen", not_found)
    assert demo.http_get("http://example.com/missing") == {"status": 404, "body": "no such page"}

    def unreachable(url, timeout):
        raise urllib.error.URLError("Name or service not known")

    monkeypatch.setattr(demo.urllib.request, "urlopen", unreachable)
    with pytest.raises(ToolError, match="request failed"):
        demo.http_get("http://nowhere.invalid/")
    with pytest.raises(ToolError, match="only http and https"):
        demo.http_get("file:///etc/passwd")


def test_http_get_bounds_max_chars():
    with pytest.raises(TypeError, match="max_chars"):
        tools.bind_args(tools.get("http_get"), {"url": "http://x", "max_chars": 0}, "run:1")


def test_send_notification_records_every_execution_and_check_finds_the_first():
    assert demo.notification_sent("run:3") is None

    first = demo.send_notification("ops", "hi", idempotency_key="run:3")
    second = demo.send_notification("ops", "hi", idempotency_key="run:3")  # the sink doesn't dedupe

    assert first["id"] != second["id"]
    assert demo.notification_sent("run:3") == first
    assert demo.notification_sent("run:4") is None


def test_demo_tools_declare_their_effects():
    names = ("echo", "calculator", "http_get", "send_notification", "publish_announcement")
    assert {n: (tools.get(n).effect.value, tools.get(n).requires_approval) for n in names} == {
        "echo": ("read_only", False), "calculator": ("read_only", False), "http_get": ("read_only", False),
        "send_notification": ("side_effect", False), "publish_announcement": ("side_effect", True)}
    assert tools.get("send_notification").check is demo.notification_sent
    assert tools.get("publish_announcement").check is demo.notification_sent



def test_demo_approval_script(client, make_worker):
    run_id = client.post("/runs", json={"goal": "announce", "model": {"provider": "scripted", "name": "demo-approval"},
                                        "tools": ["publish_announcement"]}).json()["id"]
    worker = make_worker()
    drain(worker)
    assert client.get(f"/runs/{run_id}").json()["status"] == "awaiting_approval"

    client.post(f"/runs/{run_id}/approve", json={"step_no": 1})
    drain(worker)

    run = client.get(f"/runs/{run_id}").json()
    assert (run["status"], run["result"]) == ("succeeded", "Published the announcement.")
    assert demo.notification_sent(f"{run_id}:2")["id"] == client.get(f"/runs/{run_id}/steps").json()[2]["output"]["id"]
