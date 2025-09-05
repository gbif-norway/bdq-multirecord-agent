"""
Py4J-based BDQ Service - Subprocess Py4J gateway for fast execution
"""
import logging
import time
import subprocess
import json
import tempfile
import os
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway
from py4j.protocol import Py4JNetworkError
from app.utils.logger import send_discord_notification

logger = logging.getLogger(__name__)


class BDQPy4JService:
    """
    Py4J-based BDQ Service - Subprocess gateway for fast execution
    """
    
    def __init__(self):
        self.gateway: Optional[JavaGateway] = None
        self._start_gateway()
    
    def _start_gateway(self):
        """Start Py4J gateway as subprocess"""
        java_opts = os.getenv('BDQ_JAVA_OPTS', '-Xms256m -Xmx1024m -XX:+UseSerialGC')
        gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
        
        java_cmd = ['java'] + java_opts.split() + ['-jar', gateway_jar]
        logger.info(f"Starting Py4J gateway: {' '.join(java_cmd)}")
        port = launch_gateway(java_cmd)
        self.gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        logger.info(f"Java version: {self.gateway.jvm.System.getProperty('java.version')}")
        logger.info(f"BDQ Gateway health: {self.gateway.entry_point.healthCheck()}")
    
    
    
    def execute_single_test(self, java_class: str, java_method: str, acted_upon: List[str], consulted: List[str], tuple_values: List[str]) -> Dict[str, Any]:
        """Execute a single BDQ test for a specific tuple of values"""
        try:
            # Get BDQ gateway
            bdq_gateway = self.gateway.entry_point
            
            # Execute test via Py4J gateway
            result = bdq_gateway.executeTest(
                f"{java_class}.{java_method}",  # test_id
                java_class,
                java_method,
                acted_upon,
                consulted,
                {},  # parameters, we're always going to use the defaults
                [tuple_values]  # single tuple as list
            )
            
            # Convert Java Map to Python dict
            tuple_results = list(result.get("tuple_results", []))
            errors = list(result.get("errors", []))
            
            if errors:
                logger.warning(f"Test {java_class}.{java_method} had errors: {errors}")
                return {
                    'status': 'ERROR',
                    'result': None,
                    'comment': f"Test execution error: {', '.join(errors)}",
                    'amendment': None
                }
            
            if tuple_results and len(tuple_results) > 0:
                # Return the first (and only) result
                return tuple_results[0]
            else:
                logger.warning(f"Test {java_class}.{java_method} returned no results")
                return {
                    'status': 'NO_RESULT',
                    'result': None,
                    'comment': 'Test returned no results',
                    'amendment': None
                }
                
        except Exception as e:
            logger.error(f"Error executing test {java_class}.{java_method}: {e}")
            return {
                'status': 'ERROR',
                'result': None,
                'comment': f"Test execution error: {str(e)}",
                'amendment': None
            }
    
    def shutdown(self):
        """Shutdown Py4J gateway"""
        if self.gateway:
            try:
                self.gateway.shutdown()
                logger.info("Py4J gateway connection closed")
            except Exception as e:
                logger.error(f"Error shutting down Py4J gateway connection: {e}")
            finally:
                self.gateway = None
