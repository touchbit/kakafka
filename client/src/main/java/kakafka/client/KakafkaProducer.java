/*
 * Copyright 2022 Shaburov Oleg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kakafka.client;

import kakafka.KakafkaException;
import kakafka.util.KUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
@Getter
@SuppressWarnings("unused")
public class KakafkaProducer {

    private final Properties properties;

    public KakafkaProducer(final Properties properties) {
        this.properties = properties;
    }

    public <V> void sendMessage(final String topic, final V message) {
        sendMessage(topic, UUID.randomUUID(), message, false);
    }

    public <V> void sendMessage(final String topic, final V message, final boolean wait) {
        sendMessage(topic, UUID.randomUUID(), message, wait);
    }

    public <K, V> void sendMessage(final String topic, final K key, final V message) {
        sendMessage(topic, key, message, false);
    }

    public <K, V> void sendMessage(final String topic, final K key, final V message, final boolean wait) {
        final Serializer<K> keySerializer = KUtils.getSerializer(key);
        final Serializer<V> valueSerializer = KUtils.getSerializer(message);
        sendMessage(topic, key, message, keySerializer, valueSerializer, wait);
    }

    public <K, V> void sendMessage(final String topic,
                                   final K key,
                                   final V message,
                                   final Serializer<K> keySerializer,
                                   final Serializer<V> valueSerializer,
                                   final boolean wait) {
        try (final KafkaProducer<K, V> producer = new KafkaProducer<>(properties, keySerializer, valueSerializer)) {
            final ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, message);
            final Future<RecordMetadata> future = producer.send(record);
            log.info("Sending kafka message {}", record);
            producer.flush();
            if (wait) {
                try {
                    final RecordMetadata recordMetadata = future.get();
                    log.info("Kafka message delivered.\nKey: {}\nMetadata: {}", key, recordMetadata);
                } catch (Exception err) {
                    log.error("Kafka message undelivered. Key: {}", key, err);
                    throw err;
                }
            }
        } catch (Exception exception) {
            throw KUtils.getThrowable(KakafkaException::new, "Cannot send message to kafka", exception);
        }
    }

}
