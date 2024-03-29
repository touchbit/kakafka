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

package kakafka.client;

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

@SuppressWarnings("unused")
public class KakafkaHeaders extends ArrayList<Header> {

    public KakafkaHeaders add(final String key, final String value) {
        add(new KakafkaHeader(key, value));
        return this;
    }

    public KakafkaHeaders add(String key, String value, Charset charset) {
        add(new KakafkaHeader(key, value, charset));
        return this;
    }

    public KakafkaHeaders add(final String key, final byte[] value) {
        add(new KakafkaHeader(key, value));
        return this;
    }

    public KakafkaHeaders add(final ByteBuffer keyBuffer, final ByteBuffer valueBuffer) {
        add(new KakafkaHeader(keyBuffer, valueBuffer));
        return this;
    }

}
