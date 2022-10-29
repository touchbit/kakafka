package kakafka.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Getter
@SuppressWarnings({"unused"})
/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class KakafkaConsumer {

    private final List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
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
        records.removeIf(predicate);
    }

    protected void initConsumer() {
        log.info("Subscribe for topics: {}", getTopicList());
        getConsumer().subscribe(getTopicList());
        log.info("Poll consumer at first time");
        pollRecords(getConsumer(), Duration.ofSeconds(2));
        log.info("Kafka consumer started for topics: {}", consumer.subscription());
    }

    protected void pollRecords(Consumer<String, byte[]> consumer, Duration duration) {
        ConsumerRecords<String, byte[]> records = consumer.poll(duration);
        records.forEach(record -> log.info(record.toString()));
        records.forEach(getRecords()::add);
    }

    synchronized List<ConsumerRecord<String, byte[]>> getMessages(Predicate<ConsumerRecord<String, byte[]>> p,
                                                                  boolean isRemove) {
        final List<ConsumerRecord<String, byte[]>> messages = getRecords().stream()
                .filter(p)
                .collect(Collectors.toList());
        if (isRemove) {
            getRecords().removeAll(messages);
        }
        return messages;
    }
}
