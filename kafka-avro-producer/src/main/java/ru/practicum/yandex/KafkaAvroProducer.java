package ru.practicum.yandex;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.practicum.yandex.avro.WeatherEvent;
import ru.practicum.yandex.avro.serdes.WeatherEventAvroSerializer;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaAvroProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroProducer.class);
    private static final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WeatherEventAvroSerializer.class);

        Producer<String, WeatherEvent> producer = new KafkaProducer<>(config);
        String topic = "weather-events";

        int msgCount = 100;
        log.info("Начинаю отправку {} сообщений", msgCount);

        for(int i = 0; i < msgCount; i++) {
            WeatherEvent event = getRandomMessage();
            ProducerRecord<String, WeatherEvent> record = new ProducerRecord<>(topic, event);

            producer.send(record);
        }

        producer.close();
    }

    private static WeatherEvent getRandomMessage() {
        return WeatherEvent.newBuilder()
                .setLatitude(rnd.nextDouble(-90, 90))
                .setLongitude(rnd.nextDouble(-180, 180))
                .setTemperature(rnd.nextDouble(-100, 100))
                .build();
    }
}