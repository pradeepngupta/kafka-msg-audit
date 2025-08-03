package com.pradeep.kma.audit;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CustomDateSerializer extends com.fasterxml.jackson.databind.ser.std.StdSerializer<Instant> {
    private static final long serialVersionUID = 4365897561L;

    public CustomDateSerializer(Class<Instant> t) {
        super(t);
    }

    @Override
    public void serialize(
            Instant value, JsonGenerator gen, SerializerProvider arg2)
            throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss");
        String s = dateTimeFormatter.format(LocalDateTime.ofInstant(value, ZoneId.systemDefault()));
        gen.writeString(s);
    }
}