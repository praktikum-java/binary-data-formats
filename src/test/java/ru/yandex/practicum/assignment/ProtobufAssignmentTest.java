package ru.yandex.practicum.assignment;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.proto.order.Order;
import ru.yandex.practicum.proto.order.PaymentInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtobufAssignmentTest {
    @Test
    void orderHasValidSchema() {
        Descriptors.Descriptor desc = Order.getDescriptor();
        assertEquals(desc.getFullName(), "ru.yandex.practicum.protobuf.Order");

        Descriptors.FieldDescriptor fd;

        fd = desc.findFieldByNumber(1);
        assertEquals(fd.getName(), "order_id");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.STRING);

        fd = desc.findFieldByNumber(2);
        assertEquals(fd.getName(), "product_id");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.STRING);

        fd = desc.findFieldByNumber(3);
        assertEquals(fd.getName(), "status");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.ENUM);
        Descriptors.EnumDescriptor enumType = fd.getEnumType();
        assertEquals(enumType.getName(), "OrderStatus");
        assertEquals(enumType.findValueByNumber(0).getName(), "UNKNOWN");
        assertEquals(enumType.findValueByNumber(1).getName(), "PENDING");
        assertEquals(enumType.findValueByNumber(2).getName(), "SHIPPED");
        assertEquals(enumType.findValueByNumber(3).getName(), "DELIVERED");
        assertEquals(enumType.findValueByNumber(4).getName(), "CANCELLED");

        fd = desc.findFieldByNumber(4);
        assertEquals(fd.getName(), "order_date");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.MESSAGE);
        Descriptors.Descriptor tsType = fd.getMessageType();
        assertEquals(tsType.getFullName(), "google.protobuf.Timestamp");

        fd = desc.findFieldByNumber(5);
        assertEquals(fd.getName(), "payment_info");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.MESSAGE);
        Descriptors.Descriptor paymentType = fd.getMessageType();
        assertEquals(paymentType.getName(), "PaymentInfo");

        fd = desc.findFieldByNumber(6);
        assertEquals(fd.getName(), "return_reason");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.STRING);

        Descriptors.OneofDescriptor oneof = fd.getContainingOneof();
        assertEquals(oneof.getName(), "payment_or_return");
        assertEquals(oneof.getFieldCount(), 2);
        assertEquals(oneof.getField(0).getName(), "payment_info");
        assertEquals(oneof.getField(1).getName(), "return_reason");
    }

    @Test
    void paymentInfoHasValidSchema() {
        Descriptors.Descriptor desc = PaymentInfo.getDescriptor();
        assertEquals(desc.getFullName(), "ru.yandex.practicum.protobuf.PaymentInfo");

        Descriptors.FieldDescriptor fd;

        fd = desc.findFieldByNumber(1);
        assertEquals(fd.getName(), "payment_id");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.STRING);

        fd = desc.findFieldByNumber(2);
        assertEquals(fd.getName(), "method");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.STRING);

        fd = desc.findFieldByNumber(3);
        assertEquals(fd.getName(), "amount");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.DOUBLE);

        fd = desc.findFieldByNumber(4);
        assertEquals(fd.getName(), "payment_date");
        assertEquals(fd.getType(), Descriptors.FieldDescriptor.Type.MESSAGE);
        Descriptors.Descriptor tsType = fd.getMessageType();
        assertEquals(tsType.getFullName(), "google.protobuf.Timestamp");
    }
}