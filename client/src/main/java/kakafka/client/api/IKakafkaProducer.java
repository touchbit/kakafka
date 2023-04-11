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

package kakafka.client.api;

import kakafka.client.KakafkaHeaders;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.UUID.randomUUID;

@SuppressWarnings("unused")
public interface IKakafkaProducer {

    <T> Serializer<T> getPSerializer(T tClass);

    String getTopicForProduce();

    <K, V> void send(String topic,
                     Integer partition,
                     Long ts,
                     K key,
                     V msg,
                     Collection<Header> headers,
                     Serializer<K> keySerializer,
                     Serializer<V> valueSerializer,
                     boolean wait);

    default <V> void send(V msg) {
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), true);
    }

    default <V> void send(V msg, String topic) {
        final UUID key = randomUUID();
        send(topic, null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), true);
    }

    default <V> void send(V msg, boolean wait) {
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <V> void send(V msg, boolean wait, String topic) {
        final UUID key = randomUUID();
        send(topic, null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <V> void send(V msg, Collection<Header> headers) {
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <V> void send(V msg, Consumer<Collection<Header>> consumer) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <V> void send(V msg, Collection<Header> headers, String topic) {
        final UUID key = randomUUID();
        send(topic, null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <V> void send(V msg, Consumer<Collection<Header>> consumer, boolean wait) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <V> void send(V msg, Collection<Header> headers, boolean wait) {
        final UUID key = randomUUID();
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <V> void send(V msg, Collection<Header> headers, boolean wait, String topic) {
        final UUID key = randomUUID();
        send(topic, null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(K key, V msg) {
        send(getTopicForProduce(), null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), true);
    }

    default <K, V> void send(K key, V msg, boolean wait) {
        send(getTopicForProduce(), null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(K key, V msg, String topic) {
        send(topic, null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), true);
    }

    default <K, V> void send(K key, V msg, boolean wait, String topic) {
        send(topic, null, null, key, msg, null, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(K key, V msg, Consumer<Collection<Header>> consumer) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <K, V> void send(K key, V msg, Collection<Header> headers) {
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <K, V> void send(K key, V msg, Consumer<Collection<Header>> consumer, boolean wait) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(K key, V msg, Collection<Header> headers, boolean wait) {
        send(getTopicForProduce(), null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(K key, V msg, Collection<Header> headers, String topic) {
        send(topic, null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), true);
    }

    default <K, V> void send(K key, V msg, Collection<Header> headers, boolean wait, String topic) {
        send(topic, null, null, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(Integer partition, Long ts, K key, V msg, Consumer<Collection<Header>> consumer, boolean wait) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        send(getTopicForProduce(), partition, ts, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(Integer partition, Long ts, K key, V msg, Collection<Header> headers, boolean wait) {
        send(getTopicForProduce(), partition, ts, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(Integer partition, Long ts, K key, V msg, Collection<Header> headers, boolean wait, String topic) {
        send(topic, partition, ts, key, msg, headers, getPSerializer(key), getPSerializer(msg), wait);
    }

    default <K, V> void send(Integer partition,
                             Long ts,
                             K key,
                             V msg,
                             Consumer<Collection<Header>> consumer,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             boolean wait) {
        final KakafkaHeaders headers = new KakafkaHeaders();
        consumer.accept(headers);
        send(getTopicForProduce(), partition, ts, key, msg, headers, keySerializer, valueSerializer, wait);
    }

    default <K, V> void send(Integer partition,
                             Long ts,
                             K key,
                             V msg,
                             Collection<Header> headers,
                             Serializer<K> keySerializer,
                             Serializer<V> valueSerializer,
                             boolean wait) {
        send(getTopicForProduce(), partition, ts, key, msg, headers, keySerializer, valueSerializer, wait);
    }


    default <K, V> void send(Integer partition,
                            Long ts,
                            K key,
                            V msg,
                            KakafkaHeaders headers,
                            Serializer<K> keySerializer,
                            Serializer<V> valueSerializer,
                            boolean wait) {
        send(getTopicForProduce(), partition, ts, key, msg, headers, keySerializer, valueSerializer, wait);
    }

}
