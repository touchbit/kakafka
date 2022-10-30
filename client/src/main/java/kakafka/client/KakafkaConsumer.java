package kakafka.client;

import lombok.Getter;
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

import static kakafka.util.KUtils.consumerRecordToString;

@Slf4j
@Getter
@SuppressWarnings({"unused"})
/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class KakafkaConsumer {

    private static final List<ConsumerRecord<String, byte[]>> POLLED_RECORDS = new ArrayList<>();
    private final Properties properties;
    private final List<String> topicList;
    private final Consumer<String, byte[]> consumer;
    private final ScheduledFuture<?> scheduler;

    public KakafkaConsumer(final Properties properties, final String... topics) {
        this(properties, Duration.ofMillis(200), topics);
    }

    public KakafkaConsumer(final Properties properties, final Duration poolingDuration, final String... topics) {
        this.properties = properties;
        this.consumer = new KafkaConsumer<>(getProperties());
        this.topicList = Arrays.stream(topics).collect(Collectors.toList());
        initConsumer();
        this.scheduler = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() ->
                pollRecords(getConsumer(), poolingDuration), 0, poolingDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    protected synchronized void dropMessagesByFilter(Predicate<ConsumerRecord<String, byte[]>> predicate) {
        POLLED_RECORDS.removeIf(predicate);
    }

    protected void initConsumer() {
        log.info("[KFK] Subscribe for topics: {}", getTopicList());
        getConsumer().subscribe(getTopicList());
        log.info("[KFK] Poll consumer at first time");
        pollRecords(getConsumer(), Duration.ofSeconds(2));
        log.info("[KFK] Kafka consumer started for topics: {}", consumer.subscription());
    }

    protected void pollRecords(Consumer<String, byte[]> consumer, Duration duration) {
        ConsumerRecords<String, byte[]> records = consumer.poll(duration);
        records.forEach(record -> {
            log.info("[KFK] Polled records:\n{}", consumerRecordToString(record));
            POLLED_RECORDS.add(record);
        });
    }

    synchronized List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                                  boolean isRemove) {
        final List<ConsumerRecord<String, byte[]>> messages = POLLED_RECORDS.stream()
                .filter(p)
                .collect(Collectors.toList());
        log.info("[KFK] Found {} records by filter from claimed records {}", messages.size(), POLLED_RECORDS.size());
        if (isRemove) {
            messages.forEach(record ->
                    log.info("[KFK] Message removed from static list: {}", consumerRecordToString(record)));
            POLLED_RECORDS.removeAll(messages);
        }
        return messages;
    }

    public List<ConsumerRecord<String, byte[]>> getPolledRecords() {
        return POLLED_RECORDS;
    }
}
