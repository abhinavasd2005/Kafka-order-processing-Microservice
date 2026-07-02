package com.kafka.dlqservice.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class KafkaHeaderUtil {

    private KafkaHeaderUtil() {
    }

    public static int getRetryCount(Headers headers) {

        Header header = headers.lastHeader("retryCount");

        if (header == null) {
            return 0;
        }

        return Integer.parseInt(
                new String(header.value(), StandardCharsets.UTF_8)
        );
    }

    public static LocalDateTime getNextRetryAt(Headers headers) {

        Header header = headers.lastHeader("nextRetryAt");

        if (header == null) {
            return null;
        }

        return LocalDateTime.parse(
                new String(header.value(), StandardCharsets.UTF_8),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME
        );
    }

    public static String getFailureReason(Headers headers) {

        Header header = headers.lastHeader("failureReason");

        if (header == null) {
            return null;
        }

        return new String(
                header.value(),
                StandardCharsets.UTF_8
        );
    }
    public static String getFailureType(Headers headers) {

        Header header = headers.lastHeader("failureType");

        if (header == null) {
            return null;
        }

        return new String(
                header.value(),
                StandardCharsets.UTF_8
        );

    }

}