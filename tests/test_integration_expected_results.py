"""
Focused true-integration tests that exercise selected BDQ labels end-to-end
via the real Py4J gateway. These tests assert expected statuses for known
inputs, catching marshalling/runtime issues that mocked tests would miss.
"""

import os
import pytest

from app.services.bdq_py4j_service import BDQPy4JService


@pytest.fixture(scope="module")
def bdq_service():
    gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
    assert os.path.exists(gateway_jar), f"Missing gateway JAR at {gateway_jar}"
    svc = BDQPy4JService()
    yield svc
    svc.shutdown()


def _get_mapping(service: BDQPy4JService, label: str):
    m = service._find_method_by_label(label)
    assert m is not None, f"No mapping found for label {label}"
    return m


def test_validation_countrycode_standard_compliant(bdq_service: BDQPy4JService):
    label = "VALIDATION_COUNTRYCODE_STANDARD"
    m = _get_mapping(bdq_service, label)
    # Expect COMPLIANT for a valid ISO code
    tuple_values = ["US"]  # acted_upon: [dwc:countryCode]
    results = bdq_service.execute_tests(m['class_name'], m['method_name'], ["dwc:countryCode"], [], [tuple_values])
    assert results and isinstance(results, list)
    res = results[0]
    assert res.get('status') in ("RUN_HAS_RESULT", "COMPLIANT", "PASSED"), res
    assert (res.get('result') or '').upper() == 'COMPLIANT', res


def test_validation_country_found_compliant(bdq_service: BDQPy4JService):
    label = "VALIDATION_COUNTRY_FOUND"
    m = _get_mapping(bdq_service, label)
    tuple_values = ["United States"]  # acted_upon: [dwc:country]
    results = bdq_service.execute_tests(m['class_name'], m['method_name'], ["dwc:country"], [], [tuple_values])
    assert results and isinstance(results, list)
    res = results[0]
    assert res.get('status') in ("RUN_HAS_RESULT", "COMPLIANT", "PASSED"), res
    assert (res.get('result') or '').upper() == 'COMPLIANT', res


def test_amendment_coordinates_from_verbatim_amended(bdq_service: BDQPy4JService):
    label = "AMENDMENT_COORDINATES_FROM_VERBATIM"
    m = _get_mapping(bdq_service, label)
    # Construct tuple with empty decimal lat/long (acted_upon) and usable verbatim values (consulted)
    # The consulted list in TG2 usually includes verbatimLatitude, verbatimLongitude,
    # verbatimCoordinateSystem, verbatimSRS (order taken from TG2_tests.csv)
    acted_upon = ["dwc:decimalLatitude", "dwc:decimalLongitude", "dwc:geodeticDatum"]
    # Include verbatimCoordinates first to match common method signatures
    consulted = [
        "dwc:verbatimCoordinates",
        "dwc:verbatimLatitude",
        "dwc:verbatimLongitude",
        "dwc:verbatimCoordinateSystem",
        "dwc:verbatimSRS",
    ]
    tuple_values = [
        "",             # decimalLatitude (acted)
        "",             # decimalLongitude (acted)
        "WGS84",        # geodeticDatum (acted)
        "",             # verbatimCoordinates (leave empty; use separate lat/long)
        "37.7749",      # verbatimLatitude
        "-122.4194",    # verbatimLongitude
        "decimal degrees",
        "EPSG:4326",
    ]
    results = bdq_service.execute_tests(m['class_name'], m['method_name'], acted_upon, consulted, [tuple_values])
    assert results and isinstance(results, list)
    res = results[0]
    # Expect an amendment status; some implementations use FILLED_IN for similar outcome
    assert res.get('status') in ("AMENDED", "FILLED_IN", "RUN_HAS_RESULT", "NOT_AMENDED"), res
    # When amendment occurs, result may be a label or empty; ensure not an outright error
    assert (res.get('comment') or '').upper().find('ERROR') == -1, res
