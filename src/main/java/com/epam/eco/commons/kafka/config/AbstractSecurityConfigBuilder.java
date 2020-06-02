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
package com.epam.eco.commons.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;

import com.epam.eco.commons.kafka.SslProtocol;

/**
 * @author Andrei_Tytsik
 */
public abstract class AbstractSecurityConfigBuilder<T extends AbstractConfigBuilder<T>> extends AbstractConfigBuilder<T> {

    public AbstractSecurityConfigBuilder(Map<String, Object> properties) {
        super(properties);
    }

    public T sslProtocolDefault() {
        return sslProtocol(SslConfigs.DEFAULT_SSL_PROTOCOL);
    }

    public T sslProtocol(SslProtocol sslProtocol) {
        return sslProtocol(sslProtocol.name);
    }

    public T sslProtocol(String sslProtocol) {
        return property(
                SslConfigs.SSL_PROTOCOL_CONFIG,
                sslProtocol);
    }

    public T sslProvider(String sslProvider) {
        return property(
                SslConfigs.SSL_PROVIDER_CONFIG,
                sslProvider);
    }

    public T sslCipherSuites(String ... sslCipherSuites) {
        return sslCipherSuites(String.join(",", sslCipherSuites));
    }

    public T sslCipherSuites(List<String> sslCipherSuites) {
        return sslCipherSuites(String.join(",", sslCipherSuites));
    }

    public T sslCipherSuites(String sslCipherSuites) {
        return property(
                SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                sslCipherSuites);
    }

    public T sslEnabledProtocolsDefault() {
        return sslEnabledProtocols(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS);
    }

    public T sslEnabledProtocols(SslProtocol ... sslEnabledProtocols) {
        return sslEnabledProtocols(
                Arrays.asList(sslEnabledProtocols).stream().map(p -> p.name).collect(Collectors.joining(",")));
    }

    public T sslEnabledProtocols(String ... sslEnabledProtocols) {
        return sslEnabledProtocols(String.join(",", sslEnabledProtocols));
    }

    public T sslEnabledProtocols(List<String> sslEnabledProtocols) {
        return sslEnabledProtocols(String.join(",", sslEnabledProtocols));
    }

    public T sslEnabledProtocols(String sslEnabledProtocols) {
        return property(
                SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                sslEnabledProtocols);
    }

    public T sslKeystoreTypeDefault() {
        return sslKeystoreType(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE);
    }

    public T sslKeystoreType(String sslKeystoreType) {
        return property(
                SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                sslKeystoreType);
    }

    public T sslKeystoreLocation(String keystoreLocation) {
        return property(
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                keystoreLocation);
    }

    public T sslKeystorePassword(String keystorePassword) {
        return property(
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                keystorePassword);
    }

    public T sslKeyPassword(String keyPassword) {
        return property(
                SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                keyPassword);
    }

    public T sslTruststoreTypeDefault() {
        return sslTruststoreType(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE);
    }

    public T sslTruststoreType(String sslTruststoreType) {
        return property(
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                sslTruststoreType);
    }

    public T sslTruststoreLocation(String truststoreLocation) {
        return property(
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                truststoreLocation);
    }

    public T sslTruststorePassword(String truststorePassword) {
        return property(
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                truststorePassword);
    }

    public T sslKeymanagerAlgorithmDefault() {
        return sslKeymanagerAlgorithm(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM);
    }

    public T sslKeymanagerAlgorithm(String sslKeymanagerAlgorithm) {
        return property(
                SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
                sslKeymanagerAlgorithm);
    }

    public T sslTrustmanagerAlgorithmDefault() {
        return sslTrustmanagerAlgorithm(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM);
    }

    public T sslTrustmanagerAlgorithm(String sslTrustmanagerAlgorithm) {
        return property(
                SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                sslTrustmanagerAlgorithm);
    }

    public T sslEndpointIdentificationAlgorithmDefault() {
        return sslEndpointIdentificationAlgorithm(SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
    }

    public T sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        return property(
                SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                sslEndpointIdentificationAlgorithm);
    }

    public T sslSecureRandomImplementation(String sslSecureRandomImplementation) {
        return property(
                SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
                sslSecureRandomImplementation);
    }

    public T saslMechanismDefault() {
        return saslMechanism(SaslConfigs.DEFAULT_SASL_MECHANISM);
    }

    public T saslMechanism(String saslMechanism) {
        return property(
                SaslConfigs.SASL_MECHANISM,
                saslMechanism);
    }

    public T saslJaas(String saslJaas) {
        return property(
                SaslConfigs.SASL_JAAS_CONFIG,
                saslJaas);
    }

    public <H extends AuthenticateCallbackHandler> T saslClientCallbackHandlerClass(
            Class<H> saslClientCallbackHandlerClass) {
        return saslClientCallbackHandlerClass(saslClientCallbackHandlerClass.getName());
    }

    public T saslClientCallbackHandlerClass(String saslClientCallbackHandlerClass) {
        return property(
                SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
                saslClientCallbackHandlerClass);
    }

    public <H extends AuthenticateCallbackHandler> T saslLoginCallbackHandlerClass(
            Class<H> saslLoginCallbackHandlerClass) {
        return saslLoginCallbackHandlerClass(saslLoginCallbackHandlerClass.getName());
    }

    public T saslLoginCallbackHandlerClass(String saslLoginCallbackHandlerClass) {
        return property(
                SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                saslLoginCallbackHandlerClass);
    }

    public <L extends Login> T saslLoginClass(Class<L> saslLoginClass) {
        return saslLoginClass(saslLoginClass.getName());
    }

    public T saslLoginClass(String saslLoginClass) {
        return property(
                SaslConfigs.SASL_LOGIN_CLASS,
                saslLoginClass);
    }

    public T saslKerberosServiceName(String saslKerberosServiceName) {
        return property(
                SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
                saslKerberosServiceName);
    }

    public T saslKerberosKinitCmdDefault() {
        return saslKerberosKinitCmd(SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD);
    }

    public T saslKerberosKinitCmd(String saslKerberosKinitCmd) {
        return property(
                SaslConfigs.SASL_KERBEROS_KINIT_CMD,
                saslKerberosKinitCmd);
    }

    public T saslKerberosTicketRenewWindowFactorDefault() {
        return saslKerberosTicketRenewWindowFactor(SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
    }

    public T saslKerberosTicketRenewWindowFactor(double saslKerberosTicketRenewWindowFactor) {
        return property(
                SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
                saslKerberosTicketRenewWindowFactor);
    }

    public T saslKerberosTicketRenewJitterDefault() {
        return saslKerberosTicketRenewJitter(SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER);
    }

    public T saslKerberosTicketRenewJitter(double saslKerberosTicketRenewJitter) {
        return property(
                SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
                saslKerberosTicketRenewJitter);
    }

    public T saslKerberosMinTimeBeforeReloginDefault() {
        return saslKerberosMinTimeBeforeRelogin(SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN);
    }

    public T saslKerberosMinTimeBeforeRelogin(long saslKerberosMinTimeBeforeRelogin) {
        return property(
                SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
                saslKerberosMinTimeBeforeRelogin);
    }

    public T saslLoginRefreshWindowFactorDefault() {
        return saslLoginRefreshWindowFactor(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR);
    }

    public T saslLoginRefreshWindowFactor(double saslLoginRefreshWindowFactor) {
        return property(
                SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
                saslLoginRefreshWindowFactor);
    }

    public T saslLoginRefreshWindowJitterDefault() {
        return saslLoginRefreshWindowJitter(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER);
    }

    public T saslLoginRefreshWindowJitter(double saslLoginRefreshWindowJitter) {
        return property(
                SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER,
                saslLoginRefreshWindowJitter);
    }

    public T saslLoginRefreshMinPeriodSecondsDefault() {
        return saslLoginRefreshMinPeriodSeconds(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS);
    }

    public T saslLoginRefreshMinPeriodSeconds(short saslLoginRefreshMinPeriodSeconds) {
        return property(
                SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
                saslLoginRefreshMinPeriodSeconds);
    }

    public T saslLoginRefreshBufferSecondsDefault() {
        return saslLoginRefreshBufferSeconds(SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS);
    }

    public T saslLoginRefreshBufferSeconds(short saslLoginRefreshBufferSeconds) {
        return property(
                SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
                saslLoginRefreshBufferSeconds);
    }

}
