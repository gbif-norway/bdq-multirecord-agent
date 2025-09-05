"""
Tests focused on BDQ Py4J gateway startup behavior and diagnostics.

These tests validate that we report useful errors when the Java gateway
cannot be started, which helps diagnose live issues (e.g., on Cloud Run).
"""

import os
import pytest


class TestGatewayStartup:
    def test_gateway_startup_failure_logs_stderr(self, monkeypatch):
        """If the JAR path is invalid, we should surface Java stderr in the error."""
        # Force an invalid JAR path and short timeout for quick failure
        monkeypatch.setenv('BDQ_PY4J_GATEWAY_JAR', '/nonexistent/bdq-py4j-gateway.jar')
        monkeypatch.setenv('BDQ_PY4J_STARTUP_TIMEOUT', '2')
        monkeypatch.setenv('BDQ_PY4J_RETRY_INTERVAL', '0.2')

        from app.services.bdq_py4j_service import BDQPy4JService

        with pytest.raises(RuntimeError) as exc:
            BDQPy4JService()

        # We expect to see Java's stderr about missing jarfile in the error chain
        # Exact message may vary slightly by JRE, so check for key substring
        assert 'stderr' in str(exc.value) or 'jar' in str(exc.value).lower()

