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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

    private final List<ConsumerRecord<String, byte[]>> polledRecords = synchronizedList(new ArrayList<>());
    private final Properties properties;
    private final List<String> topicList;
    private final Consumer<String, byte[]> consumer;

    public KakafkaConsumer(final Properties properties, final String... topics) {
        this(properties, Duration.ofMillis(200), topics);
    }

    public KakafkaConsumer(final Properties properties, final Duration poolingDuration, final String... topics) {
        this.properties = properties;
        this.consumer = new KafkaConsumer<>(this.properties);
        this.topicList = Arrays.stream(topics).collect(Collectors.toList());
        initConsumer();
        ScheduledFuture<?> scheduler = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() ->
                pollRecords(this.consumer, poolingDuration), 0, poolingDuration.toMillis(), TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.commitSync();
            consumer.close();
            scheduler.cancel(true);
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
            ConsumerRecords<String, byte[]> records = consumer.poll(duration);
            if (records != null) {
                records.forEach(record -> {
                    try {
                        this.polledRecords.add(record);
                    } catch (Exception e) {
                        log.error("Unable to add polled record: " + record, e);
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
        final List<ConsumerRecord<String, byte[]>> messages = this.polledRecords.stream()
                .filter(filter)
                .collect(Collectors.toList());
        if (messages.size() > 0) {
            log.info("[KFK] Found {} records by filter", messages.size());
        } else {
            log.debug("[KFK] Found 0 records by filter");
        }
        this.polledRecords.removeAll(messages);
        if (log.isTraceEnabled()) {
            messages.forEach(record ->
                    log.trace("[KFK] Message removed from polled records list:\n{}", consumerRecordToString(record)));
        }
        log.debug("[KFK] Count of deleted messages from the list of polled records: {}", messages.size());
        return messages;
    }

    public List<ConsumerRecord<String, byte[]>> getPolledRecords() {
        return polledRecords;
    }

    protected synchronized void dropMessagesByFilter(Predicate<ConsumerRecord<String, byte[]>> predicate) {
        this.polledRecords.removeIf(predicate);
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

}
