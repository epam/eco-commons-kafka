/*******************************************************************************
 *  Copyright 2022 EPAM Systems
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License.  You may obtain a copy
 *  of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *******************************************************************************/
package com.epam.eco.commons.kafka.serde;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrei_Tytsik
 */
public class JsonSerdeTest {

    private JsonSerializer serializer = new JsonSerializer();
    private JsonDeserializer<Person> deserializer = new JsonDeserializer<>();
    {
        deserializer.configure(
                Collections.singletonMap(JsonDeserializer.KEY_TYPE, Person.class), true);
    }

    @Test
    public void testDataTypeIsSerializedAndDeserialized() throws Exception {
        Person personOrig = createPerson();

        byte[] bytes = serializer.serialize(null, personOrig);

        Assert.assertNotNull(bytes);
        Assert.assertTrue(bytes.length > 0);

        Person person = deserializer.deserialize(null, bytes);

        Assert.assertEquals(personOrig, person);
    }

    @Test
    public void testNullInputGivesNullOutput() throws Exception {
        Assert.assertNull(deserializer.deserialize(null, null));
        Assert.assertNull(serializer.serialize(null, null));
    }

    @SuppressWarnings("resource")
    @Test(expected=Exception.class)
    public void testFailsOnMissingTypeConfig() throws Exception {
        new JsonDeserializer<Person>().configure(Collections.emptyMap(), true);
    }

    private Person createPerson() {
        Person person = new Person();
        person.setName("Alan Turing");
        person.setAge(41);

        Calendar calendar = Calendar.getInstance();
        calendar.set(1912, 6, 23);
        person.setBirth(calendar.getTime());

        return person;
    }

    private static class Person {

        private String name;
        private Date birth;
        private Integer age;

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        @SuppressWarnings("unused")
        public Date getBirth() {
            return birth;
        }
        public void setBirth(Date birth) {
            this.birth = birth;
        }
        @SuppressWarnings("unused")
        public Integer getAge() {
            return age;
        }
        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object obj) {
            return
                    Objects.equals(name, name) &&
                    Objects.equals(birth, birth) &&
                    Objects.equals(age, age);
        }

    }

}
