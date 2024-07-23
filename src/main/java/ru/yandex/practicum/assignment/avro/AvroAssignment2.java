package ru.yandex.practicum.assignment.avro;

import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.avro.DeviceAddedEvent;
import ru.yandex.practicum.avro.DeviceRemovedEvent;
import ru.yandex.practicum.avro.HubEvent;

import java.io.IOException;
import java.util.HexFormat;

@Slf4j
public class AvroAssignment2 {
    public static void main(String[] args) throws IOException {
        HubEvent event = deserialize(HexFormat.of().parseHex("10687562313233343594a4d8989c640010646576313233343504"));
        log.info("\n\nСкопируйте строку ниже в поле ответа на сайте Практикума:\n\n{}", getAnswer(event));
    }

    public static HubEvent deserialize(byte[] bytes) throws IOException {
        // Реализуйте метод в соответствии с заданием
        //     ...
        return null;
    }

    // Этот метод нельзя менять
    private static String getAnswer(HubEvent event) {
        return switch (event.getPayload()) {
            case DeviceAddedEvent e -> switch (e.getType()) {
                case MOTION_SENSOR -> "Unu, du, tri, kvar, kvin, la kuniklo iris promeni";
                case TEMPERATURE_SENSOR -> "Subite ĉasisto ekkuras, rekte en la kuniklon pafas";
                case LIGHT_SENSOR -> "Sed la ĉasisto ne trafis, griza kuniklo forkuris";
            };
            case DeviceRemovedEvent e -> e.getId();
            default -> "Что-то пошло не так.";
        };
    }
}