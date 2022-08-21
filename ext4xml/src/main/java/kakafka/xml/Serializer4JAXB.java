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

package kakafka.xml;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import java.io.StringWriter;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class Serializer4JAXB<O> implements Serializer<O> {

    /**
     * @param topic topic associated with data
     * @param data typed data
     * @return data byte array
     */
    @Override
    @SneakyThrows
    public byte[] serialize(String topic, O data) {
        JAXBContext jaxbContext = JAXBContext.newInstance(data.getClass());
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        StringWriter sw = new StringWriter();
        marshaller.marshal(data, sw);
        return sw.toString().getBytes();
    }

}
