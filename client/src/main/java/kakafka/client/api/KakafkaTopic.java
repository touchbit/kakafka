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

import kakafka.client.KakafkaClientBase;

/**
 * For topics enumeration
 * <p>
 *
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 * <p>
 * @see kakafka.example.Topic
 * @see kakafka.example.IncomingPaymentsKafkaClient
 * @see KakafkaClientBase#KakafkaClientBase(KakafkaTopic, KakafkaTopic...)
 */
@SuppressWarnings("JavadocReference")
public interface KakafkaTopic {

    String getName();

}
