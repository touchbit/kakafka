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

package kakafka.example;

import kakafka.client.KakafkaClientBase;
import kakafka.client.KakafkaProperties;

import static kakafka.example.Topic.*;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class IncomingPaymentsKafkaClient extends KakafkaClientBase {

    private static final KakafkaProperties PROPS = new KakafkaProperties("localhost:9092,another.host:9092")
            .withPlainSASL("login", "password")
            .withHttps(true, false);
    private static final IncomingPaymentsKafkaClient SINGLETON = new IncomingPaymentsKafkaClient();

    protected IncomingPaymentsKafkaClient() {
        super(PROPS, PAYMENT_INCOMING_TOPIC, PAYMENT_OK_TOPIC, PAYMENT_ERROR_TOPIC);
    }

    public static IncomingPaymentsKafkaClient get() {
        return SINGLETON;
    }

}
