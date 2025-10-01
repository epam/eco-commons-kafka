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
package com.epam.eco.commons.kafka;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

import kafka.coordinator.group.GroupTopicPartition;
import kafka.utils.VerifiableProperties;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author Andrei_Tytsik
 */
public abstract class ScalaConversions {

    private ScalaConversions() {
    }

    @SuppressWarnings("unchecked")
    public static <T> Seq<T> asScalaSeq(T ... values) {
        return asScalaSeq(Arrays.asList(values));
    }

    public static <T> Seq<T> asScalaSeq(List<T> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala();
    }

    public static <T> scala.collection.Set<T> asScalaSet(Set<T> set) {
        return JavaConverters.asScalaSetConverter(set).asScala();
    }

    public static <T> List<T> asJavaList(Seq<T> seq) {
        return JavaConverters.seqAsJavaListConverter(seq).asJava();
    }

    public static <T> Set<T> asJavaSet(scala.collection.Set<T> set) {
        return new HashSet<>(JavaConverters.setAsJavaSetConverter(set).asJava());
    }

    public static <KT, VT> Map<KT, VT> asJavaMap(scala.collection.Map<KT, VT> scalaMap) {
        return JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
    }

    public static TopicPartition asTopicPartition(GroupTopicPartition groupTopicPartition) {
        Validate.notNull(groupTopicPartition, "GroupTopicPartition is null");

        return new TopicPartition(
                groupTopicPartition.topicPartition().topic(),
                groupTopicPartition.topicPartition().partition());
    }

    public static <T> Optional<T> asOptional(Option<T> option) {
        Validate.notNull(option, "Option is null");

        return Optional.ofNullable(!option.isEmpty() ? option.get() : null);
    }

    public static VerifiableProperties asVerifiableProperties(Properties properties) {
        Validate.notNull(properties, "Properties object is null");

        return new VerifiableProperties(properties);
    }

    public static VerifiableProperties asVerifiableProperties(Map<String, ?> properties) {
        Validate.notNull(properties, "Properties map is null");

        Properties propertiesTmp = new Properties();
        propertiesTmp.putAll(properties);

        return new VerifiableProperties(propertiesTmp);
    }

    public static GroupState asJavaGroupState(kafka.coordinator.group.GroupState groupState) {
        Validate.notNull(groupState, "Group state is null");

        return GroupState.parse(groupState.toString());
    }

}
