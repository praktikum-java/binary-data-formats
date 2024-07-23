package ru.yandex.practicum.assignment.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.avro.DeviceAddedEvent;
import ru.yandex.practicum.avro.DeviceRemovedEvent;
import ru.yandex.practicum.avro.HubEvent;

import java.time.Instant;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class AvroAssignment2Test {

    @Test
    void mainExecutedWithoutExceptions() {
        assertDoesNotThrow(() -> AvroAssignment2.main(new String[0]));
    }

    @Test
    void deserializeDataCorrectly() {
        String hex = "1c6875623a414243233132332d3435ecaa99999c6402186465763a564f4c2335343332";

        DeviceRemovedEvent payload = DeviceRemovedEvent.newBuilder().setId("dev:VOL#5432").build();
        HubEvent expected = HubEvent.newBuilder()
                .setHubId("hub:ABC#123-45")
                .setPayload(payload)
                .setTimestamp(Instant.ofEpochMilli(1721771436726L))
                .build();

        HubEvent actual = assertDoesNotThrow(() -> AvroAssignment2.deserialize(HexFormat.of().parseHex(hex)),
                "Десериализация должна происходить без исключительных ситуаций");
        assertAll(
                () -> assertEquals(expected.getHubId(), actual.getHubId()),
                () -> assertEquals(expected.getTimestamp(), actual.getTimestamp()),
                () -> {
                    assertInstanceOf(payload.getClass(), actual.getPayload());
                    DeviceRemovedEvent removedEvent = ((DeviceRemovedEvent) actual.getPayload());
                    assertEquals(payload.getId(), removedEvent.getId());
                }
        );
    }

    @Test
    void schemaHasCorrectFieldsAndTypes() {
        Schema hubEventSchema = HubEvent.SCHEMA$;
        Field hubId = hubEventSchema.getField("hub_id");
        assertNotNull(hubId);
        assertEquals(hubId.schema().getType(), Type.STRING,
                "Поле hub_id в схеме записи HubEvent должно иметь тип string");

        Field timestamp = hubEventSchema.getField("timestamp");
        assertNotNull(timestamp);
        assertEquals(timestamp.schema().getLogicalType(), LogicalTypes.timestampMillis(),
                "Поле timestamp в схеме записи HubEvent должно иметь логический тип timestamp в миллисекундах)");

        Field payload = hubEventSchema.getField("payload");
        assertNotNull(payload);
        assertEquals(payload.schema().getType(), Schema.Type.UNION,
                "Поле payload в схеме записи HubEvent должно иметь тип union");


        Schema deviceAddedSchema = DeviceAddedEvent.SCHEMA$;
        Field id = deviceAddedSchema.getField("id");
        assertNotNull(id);
        assertEquals(id.schema().getType(), Type.STRING,
                "Поле id в схеме записи DeviceAddedEvent должно иметь тип string");

        Field type = deviceAddedSchema.getField("type");
        assertNotNull(type);
        assertEquals(type.schema().getType(), Schema.Type.ENUM,
                "Поле type в схеме записи DeviceAddedEvent должно иметь тип enum");


        Schema deviceRemovedSchema = DeviceRemovedEvent.SCHEMA$;
        Field id2 = deviceRemovedSchema.getField("id");
        assertNotNull(id2);
        assertEquals(id2.schema().getType(), Type.STRING,
                "Поле id в схеме записи DeviceRemovedEvent должно иметь тип string");
    }
}