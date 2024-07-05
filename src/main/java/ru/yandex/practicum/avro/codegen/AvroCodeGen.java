package ru.yandex.practicum.avro.codegen;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import ru.yandex.practicum.avro.Order;
import ru.yandex.practicum.avro.Product;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;

@Slf4j
public class AvroCodeGen {
    private static final HexFormat hexFormat = HexFormat.ofDelimiter(" ");

    public static void main(String[] args) throws IOException {

        Order order = getOrderRecord();

        byte[] serializedData = serializeOrderBinary(order);

        // выводим полученный массив байт в шестнадцатиричном формате
        log.info("\n\nСериализованные данные заказа (hex):\n{}\n", hexFormat.formatHex(serializedData));
        log.info("\n\nРазмер данных сериализованных в Avro: {} байт, размер данных сериализованных в JSON: {} байт\n",
                serializedData.length, serializeOrderJson(order).length);


        GenericRecord deserializedOrder = deserializeBinaryData(serializedData);

        // выводим полученные данные
        log.info("\n\nДесериализованные данные заказа:\n{}\n", deserializedOrder);
    }

    private static Order getOrderRecord() {
        // Создаём экземпляр записи в соответствии со схемой Product
        Product product1 = Product.newBuilder()
                .setProductId("p1")
                .setName("Product 1")
                .setPrice(19.99f)
                .build();

        Product product2 = Product.newBuilder()
                .setProductId("p2")
                .setName("Product 2")
                .setPrice(29.99f)
                .build();

        // Создаём и возвращаем экземпляр записи в соответствии со схемой Order
        return Order.newBuilder()
                .setOrderId("o1")
                .setCustomerName("Customer 1")
                .setProducts(Arrays.asList(product1, product2))
                .setTotalAmount(49.98f)
                .setTimestamp(Instant.now())
                .setComment("First order")
                .build();
    }

    private static byte[] serializeOrderBinary(Order order) throws IOException {

        // сериализуем данные заказа в массив байт

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); // сюда будем сохранять байты
        DatumWriter<Order> datumWriter = new SpecificDatumWriter<>(Order.class);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        // сериализуем данные заказа
        datumWriter.write(order, encoder);

        // сбрасываем все данные из буфера в поток
        encoder.flush();
        // закрываем поток
        outputStream.close();

        // преобразуем поток с записанными в него двоичными данными в массив байт
        return outputStream.toByteArray();
    }

    private static Order deserializeBinaryData(byte[] serializedData) throws IOException {
        // Десериализуем двоичные данные обратно в экземпляр класса Order

        ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedData);
        DatumReader<Order> datumReader = new SpecificDatumReader<>(Order.class);
        BinaryDecoder decoder =
                DecoderFactory.get().binaryDecoder(inputStream, null);

        // десериализуем данные заказа
        return datumReader.read(null, decoder);
    }

    private static byte[] serializeOrderJson(Order order) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); // сюда будем сохранять байты
        DatumWriter<Order> datumWriter = new SpecificDatumWriter<>(Order.class);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(Order.getClassSchema(), outputStream);
        datumWriter.write(order, encoder);
        encoder.flush();
        outputStream.close();
        return outputStream.toByteArray();
    }
}
