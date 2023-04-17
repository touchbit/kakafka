package kakafka.client;

import kakafka.client.api.IKakafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.synchronizedList;
import static kakafka.util.KUtils.consumerRecordToString;

@Slf4j
@SuppressWarnings({"unused"})
/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class KakafkaConsumer implements IKakafkaConsumer {

    private static final List<ConsumerRecord<String, byte[]>> POLLED_RECORDS = synchronizedList(new ArrayList<>());
    private final Properties properties;
    private final List<String> topicList;
    private final Consumer<String, byte[]> consumer;
    private final Thread poller;
    private final AtomicBoolean isRun = new AtomicBoolean(true);

    public KakafkaConsumer(final Properties properties, final String... topics) {
        this(properties, Duration.ofMillis(500), topics);
    }

    public KakafkaConsumer(final Properties properties, final Duration poolingDuration, final String... topics) {
        this.properties = properties;
        this.consumer = new KafkaConsumer<>(this.properties);
        this.topicList = Arrays.stream(topics).collect(Collectors.toList());
        initConsumer();
        poller = new Thread(() -> {
            while (isRun.get()) {
                pollRecords(this.consumer, poolingDuration);
            }
            this.consumer.commitSync();
            this.consumer.close();
        });
        poller.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isRun.set(false);
            try {
                poller.join(Duration.ofSeconds(1).toMillis());
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    protected void initConsumer() {
        log.info("[KFK] Subscribe for topics: {}", this.topicList);
        this.consumer.subscribe(this.topicList);
        log.info("[KFK] Poll consumer at first time");
        pollRecords(this.consumer, Duration.ofSeconds(2));
        log.info("[KFK] Kafka consumer started for topics: {}", this.consumer.subscription());
    }

    protected void pollRecords(Consumer<String, byte[]> consumer, Duration duration) {
        try {
            log.trace("[KFK] Poll consumer records with {} ms. duration", duration.toMillis());
            ConsumerRecords<String, byte[]> records = consumer.poll(duration);
            if (records != null) {
                log.trace("[KFK] Polled {} records", records.count());
                records.forEach(record -> {
                    try {
                        POLLED_RECORDS.add(record);
                    } catch (Exception e) {
                        log.error("[KFK] Unable to add polled record: " + record, e);
                    }
                    try {
                        final String stringRecord = consumerRecordToString(record);
                        log.info("[KFK] Polled record:\n{}", stringRecord);
                    } catch (Exception e) {
                        log.error("Unable to convert to string polled record: " + record, e);
                    }
                });
            }
        } catch (Exception e) {
            log.error("[KFK] Unable to read polled records", e);
        }
    }

    @Override
    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> filter) {
        final List<ConsumerRecord<String, byte[]>> messages = new ArrayList<>();
        POLLED_RECORDS.forEach(record -> {
            if (filter.test(record) && getTopicPredicate().test(record)) {
                messages.add(record);
            }
        });
        if (messages.isEmpty()) {
            log.debug("[KFK] Found 0 records by filter");
        } else {
            log.info("[KFK] Found {} records by filter", messages.size());
        }
        POLLED_RECORDS.removeAll(messages);
        if (log.isTraceEnabled()) {
            messages.forEach(record ->
                    log.trace("[KFK] Message removed from polled records list:\n{}", consumerRecordToString(record)));
        }
        log.debug("[KFK] Count of deleted messages from the list of polled records: {}", messages.size());
        return messages;
    }

    public List<ConsumerRecord<String, byte[]>> getPolledRecords() {
        return POLLED_RECORDS;
    }

    protected synchronized void dropMessagesByFilter(Predicate<ConsumerRecord<String, byte[]>> predicate) {
        POLLED_RECORDS.removeIf(predicate);
    }

    public Properties getProperties() {
        return properties;
    }

    public List<String> getTopicList() {
        return topicList;
    }

    public Consumer<String, byte[]> getConsumer() {
        return consumer;
    }

    protected Predicate<ConsumerRecord<String, byte[]>> getTopicPredicate() {
        return a -> topicList.contains(a.topic());
    }

}
