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
import kakafka.client.api.IKakafkaProducer;
import kakafka.client.api.KakafkaTopic;
import kakafka.util.KUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
@Slf4j
@Getter
@SuppressWarnings("unused")
public class KakafkaProducer implements IKakafkaProducer {

    private final Properties properties;
    private final String topicForProduce;

    public KakafkaProducer(final Properties properties, KakafkaTopic topicForProduce) {
        this(properties, topicForProduce.getName());
    }

    public KakafkaProducer(final Properties properties, String topicForProduce) {
        this.properties = properties;
        this.topicForProduce = topicForProduce;
    }

    @Override
    public <T> Serializer<T> getPSerializer(T tClass) {
        return KUtils.getSerializer(tClass);
    }

    @Override
    public String getTopicForProduce() {
        return this.topicForProduce;
    }

    public <K, V> void send(final String topic,
                            final Integer partition,
                            final Long timestamp,
                            final K key,
                            final V msg,
                            final Iterable<Header> headers,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final boolean wait) {
        try (final KafkaProducer<K, V> producer = new KafkaProducer<>(properties, keySerializer, valueSerializer)) {
            final ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, timestamp, key, msg, headers);
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
