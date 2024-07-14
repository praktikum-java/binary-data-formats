package ru.yandex.practicum.codegen;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.proto.order.Order;
import ru.yandex.practicum.proto.order.Product;

import java.io.IOException;
import java.util.HexFormat;

@Slf4j
public class ProtobufCodeGen {
    private static final HexFormat hexFormat = HexFormat.ofDelimiter(" ");

    public static void main(String[] args) throws IOException {

        Order order = getOrderMessage();

        // сериализуем данные заказа в массив байт
        byte[] serializedData = order.toByteArray();

        // выводим полученный массив байт в шестнадцатиричном формате
        log.info("\n\nСериализованные данные заказа (hex):\n{}\n", hexFormat.formatHex(serializedData));
        log.info("\n\nРазмер данных сериализованных в Protobuf: {} байт, размер данных сериализованных в JSON: {} байт\n",
                serializedData.length, serializeOrderJson(order).length);


        Order deserializedOrder = Order.parseFrom(serializedData);

        // выводим полученные данные
        log.info("\n\nДесериализованные данные заказа:\n{}\n", deserializedOrder);
    }

    private static Order getOrderMessage() {
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
                .addProducts(product1)
                .addProducts(product2)
                .setTotalAmount(49.98f)
                .setTimestamp(Timestamps.now())
                .setComment("First order")
                .build();
    }

    private static byte[] serializeOrderJson(Order order) throws IOException {
        return JsonFormat
                .printer()
                .omittingInsignificantWhitespace()
                .printingEnumsAsInts()
                .print(order)
                .getBytes();
    }
}
