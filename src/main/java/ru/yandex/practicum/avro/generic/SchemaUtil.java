package ru.yandex.practicum.avro.generic;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SchemaUtil {
    private static final String schemaJson = "{\"namespace\":\"ru.yandex.practicum.avro\","
            + "\"type\":\"record\","
            + "\"name\":\"Order\","
            + "\"fields\":[{\"name\":\"orderId\","
            + "\"type\":\"string\"},"
            + "{\"name\":\"customerName\","
            + "\"type\":\"string\"},"
            + "{\"name\":\"products\","
            + "\"type\":{\"type\":\"array\","
            + "\"items\":{\"type\":\"record\","
            + "\"name\":\"Product\","
            + "\"fields\":[{\"name\":\"productId\","
            + "\"type\":\"string\"},"
            + "{\"name\":\"name\","
            + "\"type\":\"string\"},"
            + "{\"name\":\"price\","
            + "\"type\":\"float\"}]}}},"
            + "{\"name\":\"totalAmount\","
            + "\"type\":\"float\"},"
            + "{\"name\":\"timestamp\","
            + "\"type\":{\"type\":\"long\","
            + "\"logicalType\":\"timestamp-millis\"}},"
            + "{\"name\":\"comment\","
            + "\"type\":[\"null\","
            + "\"string\"],"
            + "\"default\":null}]}";

    @Getter
    private static final Schema orderSchema;

    static {
        Schema.Parser parser = new Schema.Parser();
        orderSchema = parser.parse(schemaJson);
    }

    @Getter
    private static final Schema productSchema = orderSchema.getField("products").schema().getElementType();
}
