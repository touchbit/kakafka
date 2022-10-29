# kakafka



```java
import kakafka.client.KakafkaTopic;
import lombok.Getter;

@Getter
public enum Topic implements KakafkaTopic {

    PAYMENT_INCOMING_TOPIC("payment.incoming"),
    PAYMENT_OUTGOING_TOPIC("payment.outgoing"),
    PAYMENT_OK_TOPIC("payment.ok"),
    PAYMENT_ERROR_TOPIC("payment.error"),
    ;

    private final String name;

    Topic(String name) { this.name = name; }
}
```

```java
import kakafka.client.KakafkaClientBase;
import kakafka.client.KakafkaProperties;

import static kakafka.example.Topic.PAYMENT_ERROR_TOPIC;
import static kakafka.example.Topic.PAYMENT_INCOMING_TOPIC;
import static kakafka.example.Topic.PAYMENT_OK_TOPIC;

public class IncomingPaymentsKafkaClient extends KakafkaClientBase {

    private static final KakafkaProperties PROPS = new KakafkaProperties("localhost:9092,another.host:9092")
            .withPlainSASL("login", "password")
            .withHttps(true, false);

    protected IncomingPaymentsKafkaClient() {
        super(PROPS, PAYMENT_INCOMING_TOPIC, PAYMENT_OK_TOPIC, PAYMENT_ERROR_TOPIC);
    }
}
```