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

import kakafka.xml.data.Person;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Serializer4JAXB class unit tests")
public class Serializer4JAXBTests {

    @Test
    @DisplayName("Serializer4JAXB.serialize POJO to xml string")
    public void test1661095723590() {
        Person person = new Person();
        person.setCompanyName("test1661095723590");
        try (Serializer4JAXB<Person> serializer = new Serializer4JAXB<>()) {
            final byte[] serialize = serializer.serialize("", person);
            final String value = new String(serialize);
            assertThat(value).as("Serialized 'Person' POJO")
                    .isEqualTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                               "<Person>\n" +
                               "    <companyName>test1661095723590</companyName>\n" +
                               "</Person>\n");
        }
    }

}
