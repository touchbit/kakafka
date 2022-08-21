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

import lombok.Getter;

import java.util.Arrays;
import java.util.Properties;


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

    protected KakafkaClientBase(final Properties properties,
                                final String topic4Produce,
                                final String... topics4Consume) {
        this.topic4Produce = topic4Produce;
        this.topics4Consume = topics4Consume;
        this.properties = properties;
        // for override method
        producer = new KakafkaProducer(getProperties());
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

}
