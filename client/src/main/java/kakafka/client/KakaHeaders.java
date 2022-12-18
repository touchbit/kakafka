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
import java.util.List;

@SuppressWarnings("unused")
public class KakaHeaders {

    private final List<Header> headers = new ArrayList<>();

    public KakaHeaders add(final String key, final String value) {
        headers.add(new KakaHeader(key, value));
        return this;
    }

    public KakaHeaders add(String key, String value, Charset charset) {
        headers.add(new KakaHeader(key, value, charset));
        return this;
    }

    public KakaHeaders add(final String key, final byte[] value) {
        headers.add(new KakaHeader(key, value));
        return this;
    }

    public KakaHeaders add(final ByteBuffer keyBuffer, final ByteBuffer valueBuffer) {
        headers.add(new KakaHeader(keyBuffer, valueBuffer));
        return this;
    }

    public List<Header> getList() {
        return headers;
    }

}
