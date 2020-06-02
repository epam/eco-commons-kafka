/*
 * Copyright 2020 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.eco.commons.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Raman_Babich
 */
public class AdminClientUtilsTest {

    @Test
    public void testDescribeAllConsumerGroups() {
        AdminClient adminClient = mock(AdminClient.class);
        ListConsumerGroupsResult listConsumerGroupsResult = mock(ListConsumerGroupsResult.class);
        KafkaFutureImpl<Collection<ConsumerGroupListing>> resultFuture = new KafkaFutureImpl<>();
        resultFuture.complete(Collections.emptyList());
        when(adminClient.listConsumerGroups()).thenReturn(listConsumerGroupsResult);
        when(listConsumerGroupsResult.all()).thenReturn(resultFuture);

        Map<String, ConsumerGroupDescription> result = AdminClientUtils.describeAllConsumerGroups(adminClient);
        Assert.assertTrue(result.isEmpty());
    }

}
