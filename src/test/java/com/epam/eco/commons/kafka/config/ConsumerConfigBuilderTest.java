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
package com.epam.eco.commons.kafka.config;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.AbstractLogin.DefaultLoginCallbackHandler;
import org.apache.kafka.common.security.authenticator.DefaultLogin;
import org.apache.kafka.common.security.kerberos.KerberosClientCallbackHandler;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import com.epam.eco.commons.kafka.OffsetReset;
import com.epam.eco.commons.kafka.SslProtocol;

/**
 * @author Andrei_Tytsik
 */
public class ConsumerConfigBuilderTest {

    @Test
    public void testConfigParsed() throws Exception {
        Map<String, Object> props = ConsumerConfigBuilder.withEmpty().
                // consumer
                groupId("groupId").
                groupInstanceId("groupInstanceId").
                maxPollRecords(Integer.MAX_VALUE).
                maxPollIntervalMs(Integer.MAX_VALUE).
                sessionTimeoutMs(Integer.MAX_VALUE - 1).
                heartbeatIntervalMs(Integer.MAX_VALUE - 2).
                enableAutoCommit(true).
                partitionAssignmentStrategy(
                        Arrays.asList(
                                RangeAssignor.class,
                                StickyAssignor.class)).
                autoOffsetReset(OffsetReset.LATEST).
                fetchMinBytes(Integer.MAX_VALUE).
                fetchMaxBytes(Integer.MAX_VALUE).
                fetchMaxWaitMs(Integer.MAX_VALUE - 1).
                maxPartitionFetchBytes(Integer.MAX_VALUE).
                checkCrcs(false).
                keyDeserializer(StringDeserializer.class).
                valueDeserializer(StringDeserializer.class).
                defaultApiTimeoutMs(Integer.MAX_VALUE).
                interceptorClasses(
                        Arrays.asList(
                                TestConsumerInterceptor1.class,
                                TestConsumerInterceptor2.class)).
                excludeInternalTopics(true).
                isolationLevel(IsolationLevel.READ_COMMITTED).
                allowAutoCreateTopic(false).

                // common
                bootstrapServers("localhost:9092").
                clientDnsLookup(ClientDnsLookup.USE_ALL_DNS_IPS).
                metadataMaxAge(Long.MAX_VALUE).
                sendBuffer(Integer.MAX_VALUE).
                receiveBuffer(Integer.MAX_VALUE).
                clientId("clientId").
                clientRack("rack1").
                reconnectBackoffMs(Long.MAX_VALUE).
                reconnectBackoffMaxMs(Long.MAX_VALUE).
                retries(Integer.MAX_VALUE).
                retryBackoffMs(Long.MAX_VALUE).
                metricSampleWindowMs(Long.MAX_VALUE).
                metricNumSamples(Integer.MAX_VALUE).
                metricRecordingLevelInfo().
                metricReporterClasses(JmxReporter.class).
                securityProtocol(SecurityProtocol.PLAINTEXT).
                connectionsMaxIdleMs(Long.MAX_VALUE).
                requestTimeoutMs(Integer.MAX_VALUE).

                // ssl
                sslProtocol(SslProtocol.TLS).
                sslProvider("sun.security.provider.Sun").
                sslCipherSuites(
                        "TLS_RSA_WITH_NULL_MD5",
                        "TLS_RSA_WITH_NULL_SHA",
                        "TLS_RSA_EXPORT_WITH_RC4_40_MD5").
                sslEnabledProtocols(
                        SslProtocol.SSL,
                        SslProtocol.TLS,
                        SslProtocol.TLS_V1_2).
                sslKeystoreType("JKS").
                sslKeystoreLocation("/sslKeystoreLocation").
                sslKeystorePassword("sslKeystorePassword").
                sslKeyPassword("sslKeyPassword").
                sslTruststoreType("JKS").
                sslTruststoreLocation("/sslTruststoreLocation").
                sslTruststorePassword("sslTruststorePassword").
                sslKeymanagerAlgorithm("SunX509").
                sslTrustmanagerAlgorithm("SunX509").
                sslEndpointIdentificationAlgorithm("HTTPS").
                sslSecureRandomImplementation("SHA1PRNG").

                // sasl
                saslMechanism("GSSAPI").
                saslJaas(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"alice\"" +
                        "password=\"alice-secret\"").
                saslClientCallbackHandlerClass(KerberosClientCallbackHandler.class).
                saslLoginCallbackHandlerClass(DefaultLoginCallbackHandler.class).
                saslLoginClass(DefaultLogin.class).
                saslKerberosServiceName("kafka").
                saslKerberosKinitCmd("/usr/bin/kinit").
                saslKerberosTicketRenewWindowFactor(Double.MAX_VALUE).
                saslKerberosTicketRenewJitter(Double.MAX_VALUE).
                saslKerberosMinTimeBeforeRelogin(Long.MAX_VALUE).
                saslLoginRefreshWindowFactor(1.0).
                saslLoginRefreshWindowJitter(0.25).
                saslLoginRefreshMinPeriodSeconds((short)900).
                saslLoginRefreshBufferSeconds((short)3600).

                build();
        try (KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(props)) {
        }
    }

}
