package com.pradeep.kma.audit;

import java.util.Set;

public class Constants {
    private Constants() {
        // Prevent instantiation
    }

    public static final String INTERNAL_AUDIT_TOPIC = "audit-interceptor-topic";

    public static final Set<String> EXCLUDED_TOPICS = Set.of(INTERNAL_AUDIT_TOPIC);
    public static final String AUDIT_ID = "audit_id";


}
