/*
 * Copyright 2019 EPAM Systems
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
package com.epam.eco.commons.kafka.config;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import com.epam.eco.commons.kafka.SslProtocol;

/**
 * @author Andrei_Tytsik
 */
public class AdminClientConfigBuilderTest {

    @Test
    public void testConfigParsed() throws Exception {
        Map<String, Object> props = AdminClientConfigBuilder.withEmpty().
                // admin
                retries(Integer.MAX_VALUE).

                // common
                bootstrapServers("localhost:9092").
                metadataMaxAge(Long.MAX_VALUE).
                sendBuffer(Integer.MAX_VALUE).
                receiveBuffer(Integer.MAX_VALUE).
                clientId("clientId").
                reconnectBackoffMs(Long.MAX_VALUE).
                reconnectBackoffMaxMs(Long.MAX_VALUE).
                retryBackoffMs(Long.MAX_VALUE).
                securityProtocol(SecurityProtocol.PLAINTEXT).
                connectionMaxIdleMs(Long.MAX_VALUE).
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
                saslJaas(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"alice\"" +
                        "password=\"alice-secret\"").
                saslKerberosServiceName("kafka").
                saslKerberosKinitCmd("/usr/bin/kinit").
                saslKerberosTicketRenewWindowFactor(Double.MAX_VALUE).
                saslKerberosTicketRenewJitter(Double.MAX_VALUE).
                saslKerberosMinTimeBeforeRelogin(Long.MAX_VALUE).

                build();

        new AdminClientConfig(props);
    }

}
