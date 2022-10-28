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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;


/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
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
        if (INSTANCES.contains(this.getClass())) {
            throw new KakafkaException("Client must be singleton: " + this.getClass());
        }
        INSTANCES.add(this.getClass());
        this.topic4Produce = topic4Produce;
        this.topics4Consume = topics4Consume;
        this.properties = properties;
        final Object clientId = getProperties().get(CommonClientConfigs.CLIENT_ID_CONFIG);
        if (clientId == null || String.valueOf(clientId).isEmpty()) {
            getProperties().setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "kakafka-" + this.getClass().getSimpleName());
        }
        // for override method
        producer = new KakafkaProducer(getProperties());
        consumer = new KakafkaConsumer(getProperties(), topics4Consume);
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

    public <M> void send(final M message) {
        send(message, false);
    }

    public <M> void send(final M message, final boolean wait) {
        final String topic4Produce = getTopic4Produce();
        getProducer().sendMessage(topic4Produce, message, wait);
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
            wait -= 200;
            messages.addAll(getConsumer().getMessages(p, isRemove));
            if (messages.size() >= waitingMessageCount) {
                return messages;
            }
            try {
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
        final Predicate<ConsumerRecord<String, byte[]>> consumerRecordPredicate = record ->
                record.key().contains(searchString) || new String(record.value()).contains(searchString);
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
