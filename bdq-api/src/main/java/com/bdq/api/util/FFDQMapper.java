package com.bdq.api.util;

import com.bdq.api.model.ValidationResponse;
import org.datakurator.ffdq.api.DQResponse;

import java.lang.reflect.Method;

/**
 * Utility to convert FFDQ DQResponse to the API's ValidationResponse.
 */
public final class FFDQMapper {

    private FFDQMapper() {}

    public static ValidationResponse toValidationResponse(DQResponse<?> dq) {
        if (dq == null) {
            return new ValidationResponse("INTERNAL_PREREQUISITES_NOT_MET", "", "No response from validator");
        }
        String status = dq.getResultState() != null ? dq.getResultState().getLabel() : "";
        String result = "";
        Object value = dq.getValue();
        if (value != null) {
            // Prefer structured amendments when available: format key=value pairs with pipe delimiter.
            // Otherwise, fall back to label() for Compliance/Issue or toString().
            boolean formatted = false;
            try {
                Method getObject = value.getClass().getMethod("getObject");
                Object obj = getObject.invoke(value);
                if (obj instanceof java.util.Map<?, ?> map) {
                    StringBuilder sb = new StringBuilder();
                    boolean first = true;
                    for (Object entryObj : ((java.util.Map<?, ?>) map).entrySet()) {
                        java.util.Map.Entry<?, ?> e = (java.util.Map.Entry<?, ?>) entryObj;
                        String k = String.valueOf(e.getKey());
                        String v = e.getValue() == null ? "" : String.valueOf(e.getValue());
                        if (!first) sb.append("|");
                        sb.append(k).append("=").append(v);
                        first = false;
                    }
                    result = sb.toString();
                    formatted = true;
                }
            } catch (ReflectiveOperationException ignore) {
                // No getObject() or inaccessible; continue
            }

            if (!formatted) {
                try {
                    Method getLabel = value.getClass().getMethod("getLabel");
                    Object label = getLabel.invoke(value);
                    if (label != null) {
                        result = String.valueOf(label);
                        formatted = true;
                    }
                } catch (ReflectiveOperationException ignore) {
                    // No label; fall through
                }
            }

            if (!formatted) {
                result = String.valueOf(value);
            }
        }
        String comment = dq.getComment();
        return new ValidationResponse(status, result, comment != null ? comment : "");
    }
}
