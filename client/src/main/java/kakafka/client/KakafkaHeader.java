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

import lombok.NonNull;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KakafkaHeader extends RecordHeader {

    public KakafkaHeader(@NonNull final String key, final String value) {
        this(key, value, StandardCharsets.UTF_8);
    }

    public KakafkaHeader(@NonNull final String key, String value, @NonNull final Charset charset) {
        super(key, (value == null ? null : value.getBytes(charset)));
    }

    public KakafkaHeader(@NonNull final String key, final byte[] value) {
        super(key, value);
    }

    public KakafkaHeader(@NonNull final ByteBuffer keyBuffer, final ByteBuffer valueBuffer) {
        super(keyBuffer, valueBuffer);
    }

}
