package ru.practicum.yandex;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.practicum.yandex.avro.WeatherEvent;
import ru.practicum.yandex.avro.serdes.WeatherEventAvroDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaAvroConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroConsumer.class);

    public static void main(String[] args) {
        Properties config = new Properties();
        // эти настройки нужны, чтобы консьюмер всегда читал сообщения с самого начала топика (то есть все сообщения)
        config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // обязательные настройки
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WeatherEventAvroDeserializer.class);


        String topic = "weather-events";

        try (Consumer<String, WeatherEvent> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));

            log.info("Начинаю приём сообщений из топика: {}", topic);

            while (true) {
                ConsumerRecords<String, WeatherEvent> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, WeatherEvent> record : records) {
                    log.info("Получено сообщение из партиции {}, со смещением {}:\n{}\n",
                            record.partition(), record.offset(), record.value());
                }
            }
        }
    }
}