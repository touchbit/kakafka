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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static kakafka.util.KUtils.recordContainsString;

@SuppressWarnings("unused")
public interface IKakafkaConsumer {

    List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> filter);

    default List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> predicate,
                                                             int waitingMessageCount,
                                                             Duration waitDuration) {
        if (waitDuration == null || waitDuration.toMillis() < 200) {
            return getMessages(predicate);
        }
        long wait = waitDuration.toMillis();
        final List<ConsumerRecord<String, byte[]>> messages = new ArrayList<>();
        while (wait > 0) {
            messages.addAll(getMessages(predicate));
            if (waitingMessageCount < 1 || waitingMessageCount > messages.size()) {
                try {
                    wait -= 200;
                    Thread.sleep(200);
                    continue;
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }
            return messages;
        }
        return messages;
    }

    default List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                             Duration waitDuration) {
        return getMessages(p, -1, waitDuration);
    }

    default List<ConsumerRecord<String, byte[]>> getMessages(String searchString,
                                                             int waitingMessageCount,
                                                             Duration waitDuration) {
        Objects.requireNonNull(searchString, "Search string is required parameter");
        final Predicate<ConsumerRecord<String, byte[]>> p = record -> recordContainsString(record, searchString, true);
        return getMessages(p, waitingMessageCount, waitDuration);
    }

    default List<ConsumerRecord<String, byte[]>> getMessages(String searchString,
                                                             Duration waitDuration) {
        return getMessages(searchString, -1, waitDuration);
    }

    default List<ConsumerRecord<String, byte[]>> getMessages(String searchString) {
        return getMessages(searchString, -1, null);
    }


    default ConsumerRecord<String, byte[]> getMessage(Predicate<ConsumerRecord<String, byte[]>> predicate,
                                                      Duration waitDuration) {
        return getFirstOrNull(getMessages(predicate, 1, waitDuration));
    }

    default ConsumerRecord<String, byte[]> getMessage(String searchString,
                                                      Duration waitDuration) {
        return getFirstOrNull(getMessages(searchString, 1, waitDuration));
    }

    default ConsumerRecord<String, byte[]> getMessage(String searchString) {
        return getFirstOrNull(getMessages(searchString));
    }

    default ConsumerRecord<String, byte[]> getFirstOrNull(List<ConsumerRecord<String, byte[]>> recordList) {
        if (recordList == null || recordList.isEmpty()) {
            return null;
        }
        return recordList.get(0);
    }

}
