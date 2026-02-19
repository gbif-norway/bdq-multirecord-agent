package com.bdq.api.controller;

import com.bdq.api.model.RunTestRequest;
import com.bdq.api.model.ValidationResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TestsControllerIT {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void listTests_returnsNonEmpty() {
        ResponseEntity<Object[]> resp = rest.getForEntity(url("/api/v1/tests"), Object[].class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertTrue(resp.getBody().length > 0, "expected some discovered tests");
    }

    @Test
    void runSingle_knownValidation_notEmptyResult() {
        RunTestRequest body = new RunTestRequest();
        body.setId("VALIDATION_COUNTRY_NOTEMPTY");
        Map<String,String> params = new HashMap<>();
        params.put("dwc:country", "USA");
        body.setParams(params);

        ResponseEntity<ValidationResponse> resp = rest.postForEntity(url("/api/v1/tests/run"), body, ValidationResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        // Expect a status and some result mapping
        assertNotNull(resp.getBody().getStatus());
        assertFalse(resp.getBody().getStatus().isEmpty());
        assertNotNull(resp.getBody().getResult());
    }

    @Test
    void runBatch_mixedItems_preservesOrder_andCapturesErrors() {
        RunTestRequest ok = new RunTestRequest();
        ok.setId("VALIDATION_COUNTRY_NOTEMPTY");
        ok.setParams(Collections.singletonMap("dwc:country", "USA"));

        RunTestRequest bad = new RunTestRequest();
        bad.setId("UNKNOWN_TEST_ID");
        bad.setParams(Collections.emptyMap());

        RunTestRequest[] req = new RunTestRequest[] { ok, bad };

        ResponseEntity<ValidationResponse[]> resp = rest.postForEntity(url("/api/v1/tests/run/batch"), req, ValidationResponse[].class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals(2, resp.getBody().length);

        ValidationResponse first = resp.getBody()[0];
        assertNotNull(first.getStatus());
        assertFalse(first.getStatus().isEmpty());

        ValidationResponse second = resp.getBody()[1];
        assertEquals("INTERNAL_PREREQUISITES_NOT_MET", second.getStatus());
        assertNotNull(second.getComment());
        assertTrue(second.getComment().toLowerCase().contains("unknown test"));
    }

    @Test
    void runSingle_amendmentEventDate_standardizes() {
        RunTestRequest body = new RunTestRequest();
        body.setId("AMENDMENT_EVENTDATE_STANDARDIZED");
        Map<String,String> params = new HashMap<>();
        params.put("dwc:eventDate", "8 May 1880");
        body.setParams(params);

        ResponseEntity<ValidationResponse> resp = rest.postForEntity(url("/api/v1/tests/run"), body, ValidationResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertEquals("AMENDED", resp.getBody().getStatus());
        assertNotNull(resp.getBody().getResult());
        assertTrue(resp.getBody().getResult().startsWith("dwc:eventDate="));
        assertTrue(resp.getBody().getResult().contains("1880-05-08"));
    }

    @Test
    void runSingle_amendmentMultiField_formatsAsPipes() {
        RunTestRequest body = new RunTestRequest();
        body.setId("AMENDMENT_MINDEPTHMAXDEPTH_FROM_VERBATIM");
        Map<String,String> params = new HashMap<>();
        params.put("dwc:verbatimDepth", "10 feet");
        body.setParams(params);

        ResponseEntity<ValidationResponse> resp = rest.postForEntity(url("/api/v1/tests/run"), body, ValidationResponse.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertNotNull(resp.getBody());
        assertTrue(Set.of("AMENDED","FILLED_IN").contains(resp.getBody().getStatus()));
        String r = resp.getBody().getResult();
        assertNotNull(r);
        assertTrue(r.contains("dwc:minimumDepthInMeters=3.048"));
        assertTrue(r.contains("dwc:maximumDepthInMeters=3.048"));
        assertTrue(r.contains("|"));
        assertFalse(r.contains(" | "));
    }
}
