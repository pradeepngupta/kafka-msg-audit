package com.pradeep.kma.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditUtils {
    private AuditUtils() {
    }

    public static String getJson(AuditRecord auditRecord) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL); // remove null fields
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        ObjectWriter ow = mapper.writer();
        String json = null;
        try {
            json = ow.writeValueAsString(auditRecord);
            log.info("Publishing audit record to Kafka: {} \n", json);

        } catch (JsonProcessingException e) {
            log.error("Error serializing audit record: {}", e.getMessage(), e);
        }
        return json;
    }
}
