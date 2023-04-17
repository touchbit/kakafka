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
import kakafka.client.api.IKakafkaConsumer;
import kakafka.client.api.IKakafkaProducer;
import kakafka.client.api.KakafkaTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
@Slf4j
@SuppressWarnings("unused")
public class KakafkaClientBase implements IKakafkaProducer, IKakafkaConsumer {

    private final Properties properties;
    private final String topic4Produce;
    private final String[] topics4Consume;
    private final KakafkaProducer producer;
    private final KakafkaConsumer consumer;
    private final static List<Class<?>> INSTANCES = new ArrayList<>();

    protected KakafkaClientBase(final Properties properties,
                                final String topic4Produce,
                                final String... topics4Consume) {
        final String cliSimpleName = this.getClass().getSimpleName();
        log.info("[KFK] Init kafka client: {}", cliSimpleName);
        if (INSTANCES.contains(this.getClass())) {
            throw new KakafkaException("Client must be singleton: " + this.getClass());
        }
        INSTANCES.add(this.getClass());
        this.topic4Produce = topic4Produce;
        this.topics4Consume = topics4Consume;
        this.properties = properties;
        final Object clientId = this.properties.get(CLIENT_ID_CONFIG);
        if (clientId == null || String.valueOf(clientId).isEmpty()) {
            this.properties.setProperty(CLIENT_ID_CONFIG, "KaKafka-" + cliSimpleName);
        }
        producer = new KakafkaProducer(this.properties, this.topic4Produce);
        consumer = new KakafkaConsumer(this.properties, defaultConsumerPoolingDuration(), topics4Consume);
    }

    protected Duration defaultConsumerPoolingDuration() {
        return Duration.ofMillis(500);
    }

    protected KakafkaClientBase(final String topic4Produce,
                                final String... topics4Consume) {
        this(null, topic4Produce, topics4Consume);
    }

    protected KakafkaClientBase(final Properties properties,
                                final KakafkaTopic topic4Produce,
                                final KakafkaTopic... topics4Consume) {
        this(properties, topic4Produce.getName(), Arrays.stream(topics4Consume)
                .map(KakafkaTopic::getName).toArray(String[]::new));
    }

    protected KakafkaClientBase(final KakafkaTopic topic4Produce,
                                final KakafkaTopic... topics4Consume) {
        this(null, topic4Produce, topics4Consume);
    }

    // ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ CONSUMER ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    @Override
    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> filter) {
        return getConsumer().getMessages(filter);
    }
    // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ CONSUMER ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

    // ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ PRODUCER ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    @Override
    public <T> Serializer<T> getPSerializer(T tClass) {
        return getProducer().getPSerializer(tClass);
    }

    @Override
    public String getTopicForProduce() {
        return getProducer().getTopicForProduce();
    }

    @Override
    public <K, V> void send(String topic,
                            Integer partition,
                            Long ts,
                            K key,
                            V msg,
                            Collection<Header> headers,
                            Serializer<K> keySerializer,
                            Serializer<V> valueSerializer,
                            boolean wait) {
        getProducer().send(topic, partition, ts, key, msg, headers, keySerializer, valueSerializer, wait);
    }
    // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ PRODUCER ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

    @Override
    public String toString() {
        return "Produce topic: " + topic4Produce + "\n" +
               "Consume topics: " + Arrays.toString(topics4Consume);
    }

    public Properties getProperties() {
        return properties;
    }

    public String getTopic4Produce() {
        return topic4Produce;
    }

    public String[] getTopics4Consume() {
        return topics4Consume;
    }

    public KakafkaProducer getProducer() {
        return producer;
    }

    public KakafkaConsumer getConsumer() {
        return consumer;
    }

}
