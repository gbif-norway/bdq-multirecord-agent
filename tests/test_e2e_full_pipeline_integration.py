"""
True integration end-to-end test for the BDQ Email Processing Service.

This test exercises the real Py4J gateway and BDQ service without patching it.
It still patches external systems (LLM API and email sending) to avoid network calls.

Requirements:
- Java is installed and available on PATH
- The BDQ gateway jar exists at /opt/bdq/bdq-py4j-gateway.jar (or env BDQ_PY4J_GATEWAY_JAR points to it)
- TG2_tests.csv is available at /app/TG2_tests.csv (as in the Docker image)
"""

import base64
import os
import time
import json
import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient


class TestE2EFullPipelineIntegration:
    @pytest.fixture
    def sample_occurrence_csv(self):
        csv_path = os.path.join(os.path.dirname(__file__), "data", "simple_occurrence_dwc.csv")
        with open(csv_path, 'r') as f:
            return f.read()

    @pytest.fixture
    def email_data(self, sample_occurrence_csv):
        csv_b64 = base64.b64encode(sample_occurrence_csv.encode('utf-8')).decode('utf-8')
        return {
            "receivedAt": "2024-01-15T10:30:00Z",
            "messageId": "int-message-001",
            "threadId": "int-thread-001",
            "historyId": "999",
            "labelIds": ["INBOX"],
            "snippet": "Integration run",
            "headers": {
                "subject": "Integration Occurrence Data",
                "from": "int@example.com",
                "to": "bdq-service@example.com",
                "cc": "",
                "date": "Mon, 15 Jan 2024 10:30:00 +0000",
                "messageId": "<int-message-001@example.com>",
                "inReplyTo": "",
                "references": ""
            },
            "body": {
                "text": "Please process this occurrence data for integration testing.",
                "html": "<p>Please process this occurrence data for integration testing.</p>"
            },
            "attachments": [
                {
                    "filename": "simple_occurrence_dwc.csv",
                    "mimeType": "text/csv",
                    "size": len(sample_occurrence_csv),
                    "contentBase64": csv_b64
                }
            ]
        }

    @pytest.fixture
    def client(self):
        # Import the real app without patching BDQ service
        from app.main import app
        return TestClient(app)

    def test_full_pipeline_true_integration(self, client, email_data):
        # Fail fast if required gateway/JAR path is missing; no skip/guard
        gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
        assert os.path.exists(gateway_jar), (
            f"Expected BDQ gateway JAR at {gateway_jar}. Run inside the Docker image that contains the gateway."
        )

        # Patch only external dependencies (LLM + email send) to avoid network
        with patch('app.services.llm_service.LLMService.generate_intelligent_summary', return_value="<p>LLM summary</p>") as mock_llm, \
             patch('app.services.email_service.EmailService.send_reply', new_callable=AsyncMock) as mock_send_reply:

            # Kick off the request; the app will initialize BDQPy4JService lazily and talk to the real gateway
            resp = client.post("/email/incoming", json=email_data)
            assert resp.status_code == 200
            body = resp.json()
            assert body.get("status") == "accepted"

            # Wait for background task to finish, up to a generous timeout for gateway startup
            timeout_s = int(os.getenv('IT_MAX_WAIT_SECONDS', '60'))
            deadline = time.time() + timeout_s
            while time.time() < deadline and not mock_send_reply.called:
                time.sleep(0.5)

            assert mock_send_reply.called, "Email reply was not sent within timeout; pipeline likely failed"

            # Validate the email payload and attachments
            call = mock_send_reply.call_args
            sent_email_data = call[0][0]
            sent_body = call[0][1]
            sent_attachments = call[0][2] if len(call[0]) > 2 else None

            assert sent_email_data.get("messageId") == email_data["messageId"]
            assert "<p>" in sent_body  # HTML body present
            assert isinstance(sent_attachments, list) and len(sent_attachments) == 2

            # Decode and sanity-check raw results CSV content
            raw = next(att for att in sent_attachments if att["filename"] == "bdq_raw_results.csv")
            raw_csv = base64.b64decode(raw["contentBase64"]).decode('utf-8')
            # Should contain a header and at least one result row
            assert "test_id" in raw_csv and "status" in raw_csv
            assert raw_csv.count("\n") >= 2, f"Expected at least one result row, got: {raw_csv[:200]}"

            # Cross-check: applicable tests found and at least one of them ran
            import pandas as pd
            import io
            from app.services.csv_service import CSVService
            from app.main import get_bdq_service

            csv_service = CSVService()
            # Reconstruct the CSV content we sent to compute applicability
            sent_att = next(a for a in email_data["attachments"] if a["filename"].endswith(".csv") or a["mimeType"].startswith("text/"))
            sent_content = base64.b64decode(sent_att["contentBase64"]).decode("utf-8")
            df, core_type = csv_service.parse_csv_and_detect_core(sent_content)

            bdq = get_bdq_service()
            applicable = bdq.get_applicable_tests_for_dataset(df.columns.tolist())
            assert len(applicable) > 0, "Expected at least one applicable BDQ test for this dataset"
            applicable_labels = {t.label for t in applicable}

            # Parse raw results and ensure some of the applicable labels appear
            raw_df = pd.read_csv(io.StringIO(raw_csv))
            result_labels = set(raw_df.get('test_id', pd.Series([], dtype=str)).dropna().astype(str))
            assert result_labels, "No test_id values found in raw results"
            assert applicable_labels.intersection(result_labels), (
                f"No overlap between applicable tests ({len(applicable_labels)}) and executed tests ({len(result_labels)})"
            )

            # Amended dataset should be a valid CSV with original headers
            amended = next(att for att in sent_attachments if att["filename"] == "amended_dataset.csv")
            amended_csv = base64.b64decode(amended["contentBase64"]).decode('utf-8')
            assert "occurrenceID,eventDate,country" in amended_csv
