package com.zikeyang.contube.pulsar;

import org.apache.pulsar.common.api.proto.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class SchemaUtils {
    public static Schema convertToSchemaProto(SchemaInfo schemaInfo) {
        Schema schema = new Schema();
        schema.setName(schemaInfo.getName())
                .setSchemaData(schemaInfo.getSchema())
                .setType(getSchemaType(schemaInfo.getType()));

        schemaInfo.getProperties().entrySet().stream().forEach(entry -> {
            if (entry.getKey() != null && entry.getValue() != null) {
                schema.addProperty()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue());
            }
        });
        return schema;
    }
    private static org.apache.pulsar.common.api.proto.Schema.Type getSchemaType(SchemaType type) {
        if (type == SchemaType.AUTO_CONSUME) {
            return org.apache.pulsar.common.api.proto.Schema.Type.AutoConsume;
        } else if (type.getValue() < 0) {
            return org.apache.pulsar.common.api.proto.Schema.Type.None;
        } else {
            return org.apache.pulsar.common.api.proto.Schema.Type.valueOf(type.getValue());
        }
    }
}
