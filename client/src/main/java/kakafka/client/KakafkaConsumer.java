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
        this.consumer = new KafkaConsumer<>(getProperties());
        this.topicList = Arrays.stream(topics).collect(Collectors.toList());
        initConsumer();
        ScheduledFuture<?> scheduler = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() ->
                pollRecords(getConsumer(), poolingDuration), 0, poolingDuration.toMillis(), TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduler.cancel(true)));
    }

    protected void initConsumer() {
        log.info("[KFK] Subscribe for topics: {}", getTopicList());
        getConsumer().subscribe(getTopicList());
        log.info("[KFK] Poll consumer at first time");
        pollRecords(getConsumer(), Duration.ofSeconds(2));
        log.info("[KFK] Kafka consumer started for topics: {}", getConsumer().subscription());
    }

    protected void pollRecords(Consumer<String, byte[]> consumer, Duration duration) {
        ConsumerRecords<String, byte[]> records = consumer.poll(duration);
        records.forEach(record -> {
            log.info("[KFK] Polled record:\n{}", consumerRecordToString(record));
            getPolledRecords().add(record);
        });
    }

    @Override
    public List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> filter) {
        final List<ConsumerRecord<String, byte[]>> messages = getPolledRecords().stream()
                .filter(filter)
                .collect(Collectors.toList());
        log.info("[KFK] Found {} records by filter", messages.size());
        getPolledRecords().removeAll(messages);
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
        getPolledRecords().removeIf(predicate);
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
