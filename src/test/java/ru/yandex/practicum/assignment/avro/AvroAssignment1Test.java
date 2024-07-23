package ru.yandex.practicum.assignment.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.avro.Gender;
import ru.yandex.practicum.avro.User;

import java.time.LocalDate;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AvroAssignment1Test {

    @Test
    void mainExecutedWithoutExceptions() {
        assertDoesNotThrow(() -> AvroAssignment1.main(new String[0]));
    }

    @Test
    void userFieldsHaveCorrectTypes() {
        User user = AvroAssignment1.getUser();
        assertAll(
                () -> assertInstanceOf(List.class, user.getInterests(), "Поле interests должно иметь тип List"),
                () -> assertInstanceOf(String.class, user.getFirstName(), "Поле firstName должно иметь тип String"),
                () -> assertInstanceOf(Map.class, user.getContacts(), "Поле contacts должно иметь тип Map"),
                () -> assertInstanceOf(String.class, user.getLastName(), "Поле lastName должно иметь тип String"),
                () -> assertInstanceOf(Enum.class, user.getGender(), "Поле gender должно иметь тип Enum"),
                () -> assertNull(user.getMiddleName(), "Поле middleName должно быть равно null"),
                () -> assertInstanceOf(Integer.class, user.getId(), "Поле id должно иметь тип Integer"),
                () -> assertInstanceOf(LocalDate.class, user.getBirthdate(), "Поле id должно иметь тип LocalDate")
        );
    }

    @Test
    void serializeDataCorrectly() {
        String expected = "d00f0a5465737431020a54657374320a546573743302107061696e74696e670086a10202020a4972696e61242b37202838373929203132342d39382d373400";

        User user = User.newBuilder()
                .setInterests(List.of("painting"))
                .setFirstName("Test1")
                .setContacts(Map.of("Irina", "+7 (879) 124-98-74"))
                .setLastName("Test3")
                .setBirthdate(LocalDate.of(2020, 8, 25))
                .setGender(Gender.female)
                .setMiddleName("Test2")
                .setId(1000)
                .build();

        byte[] result = assertDoesNotThrow(() -> AvroAssignment1.serialize(user),
                "Сериализация должна происходить без исключительных ситуаций");

        String actual = HexFormat.of().formatHex(result);
        assertEquals(expected, actual, "Сериализованные данные должны соответствовать ожидаемым");
    }

    @Test
    void schemaHasCorrectFieldsAndTypes() {
        Schema schema = User.SCHEMA$;
        Field id = schema.getField("id");
        assertNotNull(id);
        assertEquals(id.schema().getType(), Schema.Type.INT,
                "Поле id в схеме записи User должно иметь тип int");

        Field firstName = schema.getField("firstName");
        assertNotNull(firstName);
        assertEquals(firstName.schema().getType(), Schema.Type.STRING,
                "Поле firstName в схеме записи User должно иметь тип string");

        Field middleName = schema.getField("middleName");
        assertNotNull(middleName);
        assertEquals(middleName.schema().getType(), Schema.Type.UNION,
                "Поле middleName в схеме записи User должно иметь тип union");
        assertTrue(middleName.hasDefaultValue());

        Field lastName = schema.getField("lastName");
        assertNotNull(lastName);
        assertEquals(lastName.schema().getType(), Schema.Type.STRING,
                "Поле lastName в схеме записи User должно иметь тип string");

        Field interests = schema.getField("interests");
        assertNotNull(interests);
        assertEquals(interests.schema().getType(), Schema.Type.ARRAY,
                "Поле interests в схеме записи User должно иметь тип array");
        assertEquals(interests.schema().getElementType().getType(), Schema.Type.STRING,
                "Элементы поля interests в схеме записи User должны иметь тип string");

        Field birthdate = schema.getField("birthdate");
        assertNotNull(birthdate);
        assertEquals(birthdate.schema().getLogicalType(), LogicalTypes.date(),
                "Поле birthdate в схеме записи User должно иметь логический тип date)");

        Field gender = schema.getField("gender");
        assertNotNull(gender);
        assertEquals(gender.schema().getType(), Schema.Type.ENUM,
                "Поле gender в схеме записи User должно иметь тип enum");

        Field contacts = schema.getField("contacts");
        assertNotNull(contacts);
        assertEquals(contacts.schema().getType(), Schema.Type.MAP,
                "Поле contacts в схеме записи User должно иметь тип map");
        assertEquals(contacts.schema().getValueType().getType(), Schema.Type.STRING,
                "Элементы поля contacts в схеме записи User должны иметь тип string");
    }
}