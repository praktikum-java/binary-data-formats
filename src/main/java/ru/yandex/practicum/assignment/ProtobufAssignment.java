package ru.yandex.practicum.assignment;

import com.google.protobuf.util.Timestamps;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.proto.order.Order;
import ru.yandex.practicum.proto.order.OrderStatus;

import java.io.IOException;
import java.util.HexFormat;

@Slf4j
public class ProtobufAssignment {
    private static final HexFormat hexFormat = HexFormat.of();
    private static final String hexString = "0a116f726465722d37382f3230323430373235120970726f647563742d31120970726f647563742d32120970726f647563742d331802220a08e28687b50610c0843d2a3b0a167061796d656e742d3132335f323032342f30372f3235120b63726564697420636172641900000000004a9340220b0884f087b50610c0de810a";

    public static void main(String[] args) throws IOException {
        Order order = getOrderMessage();
        byte[] bytes = hexFormat.parseHex(hexString);

        //----------------------------------------------------------------
        // Доработайте код ниже
        //----------------------------------------------------------------

        // сериализуйте экземпляр класса Order сохраненный в переменной order
        byte[] serializedData = ...;
        // десериализуйте массив байт из переменной bytes
        Order deserializedOrder = ...;

        //----------------------------------------------------------------

        // Этот код менять нельзя
        log.info("\n\nСкопируйте строку ниже в поле ответа на сайте Практикума:\n\n{}:{}:{}:{}\n",
                hexFormat.formatHex(serializedData),
                deserializedOrder.getProductIdCount(),
                deserializedOrder.getPaymentInfo().getPaymentId(),
                Timestamps.toMillis(deserializedOrder.getOrderDate())
        );
    }

    // Этот метод менять нельзя
    private static Order getOrderMessage() {
        return Order.newBuilder()
                .setOrderId("order-5/20200217")
                .addProductId("product-1")
                .setReturnReason("Недостаточно средств")
                .setStatus(OrderStatus.CANCELLED)
                .setOrderDate(Timestamps.parseUnchecked("2020-02-16T09:51:46.013+03:00"))
                .build();
    }
}
