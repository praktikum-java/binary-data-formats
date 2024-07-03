package ru.yandex.practicum.avro.generic;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;

@Slf4j
public class AvroGenericSerDe {
    private static final HexFormat hexFormat = HexFormat.ofDelimiter(" ");

    public static void main(String[] args) throws IOException {

        GenericRecord orderRecord = getOrderRecord();

        byte[] serializedData = serializeOrderBinary(orderRecord);

        // выводим полученный массив байт в шестнадцатиричном формате
        log.info("\n\nСериализованные данные заказа (hex):\n{}\n", hexFormat.formatHex(serializedData));
        log.info("\n\nРазмер данных сериализованных в Avro: {} байт, размер данных сериализованных в JSON: {} байт\n",
                serializedData.length, serializeOrderJson(orderRecord).length);


        GenericRecord deserializedOrder = deserializeBinaryData(serializedData);

        // выводим полученные данные
        log.info("\n\nДесериализованные данные заказа:\n{}\n", deserializedOrder);
    }

    private static GenericRecord getOrderRecord() {
        // Создаём экземпляр записи в соответствии со схемой Product
        GenericRecord product1 = new GenericRecordBuilder(SchemaUtil.getProductSchema())
                .set("productId", "p1")
                .set("name", "Product 1")
                .set("price", 19.99f)
                .build();

        GenericRecord product2 = new GenericRecordBuilder(SchemaUtil.getProductSchema())
                .set("productId", "p2")
                .set("name", "Product 2")
                .set("price", 29.99f)
                .build();

        // Создаём и возвращаем экземпляр записи в соответствии со схемой Order
        return new GenericRecordBuilder(SchemaUtil.getOrderSchema())
                .set("orderId", "o1")
                .set("customerName", "Customer 1")
                .set("products", Arrays.asList(product1, product2))
                .set("totalAmount", 49.98f)
                .set("timestamp", Instant.now().toEpochMilli())
                .set("comment", "First order")
                .build();
    }

    private static byte[] serializeOrderBinary(GenericRecord orderRecord) throws IOException {

        // сериализуем данные заказа в массив байт

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); // сюда будем сохранять байты

        // создаём экземпляр GenericDatumWriter - он умеет сериализовать данные используя схему
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SchemaUtil.getOrderSchema());
        // avro поддерживает сериализацию в json (удобно для отладки) и собственно в двоичный формат
        // получаем экземпляр BinaryEncoder, который будет сериализовать данные именно в двоичный формат
        // и записывать результат в подготовленный нами ByteArrayOutputStream
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // сериализуем данные заказа
        datumWriter.write(orderRecord, encoder);

        // сбрасываем все данные из буфера в поток
        encoder.flush();
        // закрываем поток
        outputStream.close();

        // преобразуем поток с записанными в него двоичными данными в массив байт
        return outputStream.toByteArray();
    }

    private static GenericRecord deserializeBinaryData(byte[] serializedData) throws IOException {
        // Десериализуем двоичные данные обратно в экземпляр класса Order

        // создаём байтовый поток из которого десериализатор будет брать двоичные данные
        ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedData);

        // создаём экземпляр GenericDatumReader - он умеет десериализовывать данные используя схему
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SchemaUtil.getOrderSchema());

        // так как avro поддерживает сериализацию в json и в двоичный формат, то
        // получаем экземпляр BinaryDecoder, который умеет работать именно с двоичными
        // данными, которые он будет читать из потока inputStream
        BinaryDecoder decoder =
                DecoderFactory.get().binaryDecoder(inputStream, null);

        // десериализуем данные заказа
        return datumReader.read(null, decoder);
    }

    private static byte[] serializeOrderJson(GenericRecord orderRecord) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); // сюда будем сохранять байты
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SchemaUtil.getOrderSchema());
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(SchemaUtil.getOrderSchema(), outputStream);
        datumWriter.write(orderRecord, encoder);
        encoder.flush();
        outputStream.close();
        return outputStream.toByteArray();
    }
}