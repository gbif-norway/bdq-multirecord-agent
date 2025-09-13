import pytest
from app.services.llm_service import LLMService


class FakeResponses:
    def __init__(self, recorder, output_text):
        self.recorder = recorder
        self._output_text = output_text

    def create(self, model=None, input=None, **kwargs):
        # Record call for assertions (model + presence of extra kwargs)
        self.recorder.append(("responses.create", model, kwargs))

        class _Resp:
            def __init__(self, text):
                self.output_text = text

        return _Resp(self._output_text)


class FakeOpenAIClient:
    def __init__(self, recorder, output_text="Hello world"):
        self._recorder = recorder
        self.responses = FakeResponses(self._recorder, output_text)


def test_generate_openai_summary_uses_gpt5_and_formats_html(monkeypatch):
    calls = []

    # Patch OpenAI constructor used inside LLMService
    def _fake_ctor(api_key=None):
        return FakeOpenAIClient(calls, output_text="This is a plain response without html tags.")

    monkeypatch.setattr("app.services.llm_service.OpenAI", _fake_ctor)

    svc = LLMService()

    html = svc.generate_openai_intelligent_summary(
        prompt="Test prompt",
        test_results_csv_text="a,b\n1,2",
        original_csv_text="x,y\n3,4",
        curated_csv_text="c,d\n5,6",
        recipient_name="Rukaya",
    )

    # Assert Responses API called with gpt-5
    assert any(call[0] == "responses.create" and call[1] == "gpt-5" for call in calls), f"responses.create not called with gpt-5: {calls}"


