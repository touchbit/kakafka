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

package kakafka.util;

import kakafka.KakafkaException;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
@SuppressWarnings("DuplicatedCode")
@UtilityClass
public class KUtils {

    public static String consumerRecordToString(final ConsumerRecord<String, byte[]> record) {
        if (record != null) {
            final StringJoiner headers = new StringJoiner("\n - ");
            record.headers().forEach(h -> {
                final byte[] bytesValue = h.value();
                if (bytesValue != null) {
                    if (bytesValue.length > 0) {
                        headers.add(h.key() + "=" + new String(bytesValue));
                    } else {
                        headers.add(h.key() + "=");
                    }
                }
            });
            StringJoiner result = new StringJoiner("\n");
            result.add("topic: " + record.topic());
            result.add("partition: " + record.partition());
            result.add(record.timestampType() + ": " + record.timestamp());
            result.add("headers: " + (headers.length() == 0 ? "[]" : "\n - " + headers));
            result.add("key: " + record.key());
            final byte[] byteBody = record.value();
            if (byteBody != null) {
                result.add("value: " + new String(record.value()));
            } else {
                result.add("value: <not present (null)>");
            }
            return result.toString();
        }
        return "null";
    }

    @SuppressWarnings({"unchecked", "rawtypes" })
    public static <T> Serializer<T> getSerializer(T tClass) {
        Objects.requireNonNull(tClass);
        try {
            if (tClass instanceof UUID) {
                return (Serializer<T>) new Serializer4UUID();
            }
            if (tClass instanceof Number) {
                return new Serializer4Number();
            }
            if (tClass instanceof com.google.protobuf.MessageLite) {
                return new kakafka.grpc.Serializer4Proto();
            }
            if (tClass instanceof String) {
                return (Serializer<T>) new StringSerializer();
            }
        } catch (Throwable t) {
            String message = t.getMessage();
            if (message.contains("Serializer4Proto")) {
                throw new KakafkaException("\n\nKafkaProtoSerializer class not found. Add 'ext4grpc' dependency to maven/gradle configuration file\n\n" +
                        "Gradle:\nimplementation group: 'org.touchbit.kakafka', name: 'ext4grpc', version: '1.0.0'\n\n" +
                        "Maven:\n" +
                        "<dependency>\n" +
                        "    <groupId>org.touchbit.kakafka</groupId>\n" +
                        "    <artifactId>ext4grpc</artifactId>\n" +
                        "    <version>1.0.0</version>\n" +
                        "    <scope>compile</scope>\n" +
                        "</dependency>", true);
            }
            throw t;
        }
        throw new KakafkaException("Serializer not found for class " + tClass.getClass());
    }

    public static <R extends Throwable> R getThrowable(BiFunction<String, Throwable, R> supplier,
                                                       String message,
                                                       Throwable throwable) {
        final String assertionMessage;
        if (throwable != null) {
            final List<String> throwableCauses = getThrowableCauses(throwable);
            if (!throwableCauses.isEmpty()) {
                assertionMessage = message;
            } else {
                String causes = String.join("\n  - ", throwableCauses);
                assertionMessage = message + "\nCauses:\n  - " + causes;
            }
        } else {
            assertionMessage = message;
        }
        return supplier.apply(assertionMessage, throwable);
    }

    public static List<String> getThrowableCauses(Throwable throwable) {
        final List<String> result = new ArrayList<>();
        String message = throwable.getMessage();
        result.add(message);
        Throwable cause = throwable.getCause();
        if (cause != null) {
            result.addAll(getThrowableCauses(cause));
        }
        return result;
    }

    public static boolean recordContainsString(ConsumerRecord<String, byte[]> record, String s, boolean ignoreCase) {
        final boolean headersContainsString = headersContainsString(record.headers(), s, ignoreCase);
        if (headersContainsString) {
            return true;
        }
        if (ignoreCase) {
            return ((record.key() != null && record.key().toLowerCase().contains(s.toLowerCase()))
                    || (record.value() != null && new String(record.value()).toLowerCase().contains(s.toLowerCase())));
        }
        return (record.key() != null && record.key().contains(s))
               || (record.value() != null && new String(record.value()).contains(s));
    }

    public static boolean headersContainsString(Headers headers, String s, boolean ignoreCase) {
        return StreamSupport
                .stream(headers.spliterator(), false)
                .anyMatch(h -> headerContainsString(h, s, ignoreCase));
    }

    public static boolean headerContainsString(Header header, String s, boolean ignoreCase) {
        if (ignoreCase) {
            return (header.key() != null && header.key().toLowerCase().contains(s.toLowerCase())) ||
                   (header.value() != null && new String(header.value()).toLowerCase().contains(s.toLowerCase()));
        }
        return (header.key() != null && header.key().contains(s)) ||
               (header.value() != null && new String(header.value()).contains(s));
    }

}
