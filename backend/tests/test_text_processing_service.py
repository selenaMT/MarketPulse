from types import SimpleNamespace

import pytest

from app.services.text_processing_service import TextProcessingService


class FakeResponsesAPI:
    def __init__(self, response):
        if isinstance(response, list):
            self._responses = list(response)
        else:
            self._responses = [response]
        self.last_call = None

    def create(self, *, model, input, text):
        self.last_call = {"model": model, "input": input, "text": text}
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


class FakeClient:
    def __init__(self, response):
        self.responses = FakeResponsesAPI(response)


def test_init_raises_without_api_key_and_without_client(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        TextProcessingService(api_key=None, client=None)


def test_process_uses_default_model_and_returns_dict():
    payload = '{"event":"US CPI rose higher than expected","entities":[],"region":"US","policy_signal":"hawkish","asset_impacts":[],"relationships":[],"keep":true}'
    fake_client = FakeClient(SimpleNamespace(output_text=payload))
    service = TextProcessingService(client=fake_client, default_model="model-default")

    result = service.process("Title: Inflation update")

    assert fake_client.responses.last_call["model"] == "model-default"
    assert result["event"] == "US CPI rose higher than expected"
    assert result["keep"] is True


def test_process_allows_model_override():
    payload = '{"event":"Fed signals possible rate cuts","entities":[],"region":"US","policy_signal":"dovish","asset_impacts":[],"relationships":[],"keep":true}'
    fake_client = FakeClient(SimpleNamespace(output_text=payload))
    service = TextProcessingService(client=fake_client, default_model="model-default")

    result = service.process("Some text", model="model-override")

    assert fake_client.responses.last_call["model"] == "model-override"
    assert result["policy_signal"] == "dovish"


def test_process_raises_on_blank_text():
    payload = '{"event":"","entities":[],"region":"Global","policy_signal":"neutral","asset_impacts":[],"relationships":[],"keep":false}'
    fake_client = FakeClient(SimpleNamespace(output_text=payload))
    service = TextProcessingService(client=fake_client)

    with pytest.raises(ValueError, match="non-empty"):
        service.process("   ")


def test_process_retries_invalid_json_and_succeeds():
    invalid = SimpleNamespace(output_text="{not-json")
    valid = SimpleNamespace(
        output_text='{"event":"Fed update","entities":[],"region":"US","policy_signal":"neutral","asset_impacts":[],"relationships":[],"keep":true}'
    )
    fake_client = FakeClient([invalid, valid])
    service = TextProcessingService(client=fake_client, invalid_json_retries=2)

    result = service.process("Fed text")

    assert result["event"] == "Fed update"
    assert result["keep"] is True


def test_process_raises_after_retry_limit_exceeded():
    invalid = SimpleNamespace(output_text="{still-not-json")
    fake_client = FakeClient([invalid, invalid, invalid])
    service = TextProcessingService(client=fake_client, invalid_json_retries=2)

    with pytest.raises(ValueError, match="Failed to parse valid JSON"):
        service.process("Some article text")
