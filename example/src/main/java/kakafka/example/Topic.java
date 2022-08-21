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

import kakafka.client.KakafkaTopic;
import lombok.Getter;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
@Getter
public enum Topic implements KakafkaTopic {

    DLQ("dead-letter-queue"),
    PAYMENT_INCOMING("payment.incoming"),
    PAYMENT_OUTGOING("payment.outgoing"),
    PAYMENT_RESULT("payment.result"),
    PAYMENT_ERROR("payment.error"),
    ;

    private final String name;

    Topic(String name) {
        this.name = name;
    }

}
