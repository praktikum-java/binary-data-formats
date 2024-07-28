package ru.practicum.yandex.avro.serdes;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import ru.practicum.yandex.avro.WeatherEvent;

public class WeatherEventAvroDeserializer implements Deserializer<WeatherEvent> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private final DatumReader<WeatherEvent> reader = new SpecificDatumReader<>(WeatherEvent.getClassSchema());


    @Override
    public WeatherEvent deserialize(String topic, byte[] data) {
        try {
            if (data != null) {
                BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
                return this.reader.read(null, decoder);
            }
            return null;
        } catch (Exception e) {
            throw new DeserializationException("Ошибка десереализации данных из топика [" + topic + "]", e);
        }
    }
}