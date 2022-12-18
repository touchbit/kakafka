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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
@Slf4j
@Getter
@SuppressWarnings("unused")
public abstract class KakafkaClientBase {

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
        final Object clientId = this.properties.get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (clientId == null || String.valueOf(clientId).isEmpty()) {
            this.properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "KaKafka-" + cliSimpleName);
        }
        producer = new KakafkaProducer(this.properties);
        consumer = new KakafkaConsumer(this.properties, topics4Consume);
    }

    protected KakafkaClientBase(final String topic4Produce,
                                final String... topics4Consume) {
        this(null, topic4Produce, topics4Consume);
    }

    protected KakafkaClientBase(final Properties properties,
                                final KakafkaTopic topic4Produce,
                                final KakafkaTopic... topics4Consume) {
        this(properties, topic4Produce.getName(), Arrays.stream(topics4Consume).map(KakafkaTopic::getName).toArray(String[]::new));
    }

    protected KakafkaClientBase(final KakafkaTopic topic4Produce,
                                final KakafkaTopic... topics4Consume) {
        this(null, topic4Produce, topics4Consume);
    }

    public <V> void send(V message) {
        getProducer().sendMessage(getTopic4Produce(), message);
    }

    public <V> void send(V message, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), message, wait);
    }

    public <V> void send(V message, Consumer<KakaHeaders> consumer) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), message, headers.getList());
    }

    public <V> void send(V message, KakaHeaders headers) {
        getProducer().sendMessage(getTopic4Produce(), message, headers.getList());
    }

    public <V> void send(V message, Consumer<KakaHeaders> consumer, boolean wait) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), message, headers.getList(), wait);
    }

    public <V> void send(V message, KakaHeaders headers, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), message, headers.getList(), wait);
    }

    public <K, V> void send(K key, V message) {
        getProducer().sendMessage(getTopic4Produce(), key, message);
    }

    public <K, V> void send(K key, V message, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), key, message, wait);
    }

    public <K, V> void send(K key, V message, Consumer<KakaHeaders> consumer) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), key, message, headers.getList());
    }

    public <K, V> void send(K key, V message, KakaHeaders headers) {
        getProducer().sendMessage(getTopic4Produce(), key, message, headers.getList());
    }

    public <K, V> void send(K key, V message, Consumer<KakaHeaders> consumer, boolean wait) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), key, message, headers.getList(), wait);
    }

    public <K, V> void send(K key, V message, KakaHeaders headers, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), key, message, headers.getList(), wait);
    }

    public <K, V> void send(Integer partition, Long timestamp, K key, V message, Consumer<KakaHeaders> consumer, boolean wait) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), partition, timestamp, key, message, headers.getList(), wait);
    }

    public <K, V> void send(Integer partition, Long timestamp, K key, V message, KakaHeaders headers, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), partition, timestamp, key, message, headers.getList(), wait);
    }

    public <K, V> void send(Integer partition, Long timestamp, K key, V message, Consumer<KakaHeaders> consumer, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean wait) {
        final KakaHeaders headers = new KakaHeaders();
        consumer.accept(headers);
        getProducer().sendMessage(getTopic4Produce(), partition, timestamp, key, message, headers.getList(), keySerializer, valueSerializer, wait);
    }

    public <K, V> void send(Integer partition, Long timestamp, K key, V message, KakaHeaders headers, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean wait) {
        getProducer().sendMessage(getTopic4Produce(), partition, timestamp, key, message, headers.getList(), keySerializer, valueSerializer, wait);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                            boolean isRemove,
                                                            int waitingMessageCount,
                                                            Duration waitDuration) {
        long wait = waitDuration.toMillis();
        final List<ConsumerRecord<String, byte[]>> messages = new ArrayList<>();
        if (wait < 200) {
            return getConsumer().getMessages(p, isRemove);
        }
        while (wait > 0) {
            messages.addAll(getConsumer().getMessages(p, isRemove));
            if (messages.size() >= waitingMessageCount) {
                return messages;
            }
            try {
                wait -= 200;
                Thread.sleep(200);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
        return messages;
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                            boolean isRemove,
                                                            Duration waitDuration) {
        return getMessages(p, isRemove, 1, waitDuration);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                            Duration waitDuration) {
        return getMessages(p, false, 1, waitDuration);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p) {
        return getMessages(p, false, 1, defaultWaitDuration());
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(String searchString,
                                                            boolean isRemove,
                                                            int waitingMessageCount,
                                                            Duration waitDuration) {
        Objects.requireNonNull(searchString, "Search string is required parameter");
        Objects.requireNonNull(waitDuration, "Wait duration is required parameter");
        final Predicate<ConsumerRecord<String, byte[]>> consumerRecordPredicate = record ->
                StreamSupport.stream(record.headers().spliterator(), false)
                        .anyMatch(h ->
                                (h.key() != null && h.key().contains(searchString))
                                        || (h.value() != null && new String(h.value()).contains(searchString)))
                        || (record.key() != null && record.key().contains(searchString))
                        || (record.value() != null && new String(record.value()).contains(searchString));
        return getMessages(consumerRecordPredicate, isRemove, waitingMessageCount, waitDuration);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(String searchString,
                                                            boolean isRemove,
                                                            Duration waitDuration) {
        return getMessages(searchString, isRemove, 1, waitDuration);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(String searchString,
                                                            Duration waitDuration) {
        return getMessages(searchString, false, 1, waitDuration);
    }

    public List<ConsumerRecord<String, byte[]>> getMessages(String searchString) {
        return getMessages(searchString, false, 1, defaultWaitDuration());
    }

    protected Duration defaultWaitDuration() {
        return Duration.ofSeconds(60);
    }

    @Override
    public String toString() {
        return "Produce topic: " + topic4Produce + "\n" +
                "Consume topics: " + Arrays.toString(topics4Consume);
    }
}
