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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.function.BiFunction;

public class KUtils {

    @SuppressWarnings({"unchecked", "rawtypes"})
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

}
