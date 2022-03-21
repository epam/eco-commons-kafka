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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * @author Andrei_Tytsik
 */
public abstract class AbstractClientConfigBuilder<T extends AbstractSecurityConfigBuilder<T>> extends AbstractSecurityConfigBuilder<T> {

    public AbstractClientConfigBuilder(Map<String, Object> properties) {
        super(properties);
    }

    public T bootstrapServers(String bootstrapServers) {
        return property(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
    }

    public T clientDnsLookup(ClientDnsLookup clientDnsLookup) {
        return clientDnsLookup(clientDnsLookup.toString());
    }

    public T clientDnsLookup(String clientDnsLookup) {
        return property(
                CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                clientDnsLookup);
    }

    public T metadataMaxAge(long metadataMaxAge) {
        return property(
                CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                metadataMaxAge);
    }

    public T sendBufferOsDefault() {
        return sendBuffer(-1);
    }

    public T sendBuffer(int sendBuffer) {
        return property(
                CommonClientConfigs.SEND_BUFFER_CONFIG,
                sendBuffer);
    }

    public T receiveBufferOsDefault() {
        return receiveBuffer(-1);
    }

    public T receiveBuffer(int receiveBuffer) {
        return property(
                CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                receiveBuffer);
    }

    public T clientIdRandom() {
        return clientId(UUID.randomUUID().toString());
    }

    public T clientIdRandomIfAbsent() {
        return propertyIfAbsent(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                () -> UUID.randomUUID().toString());
    }

    public T clientId(String clientId) {
        return property(
                CommonClientConfigs.CLIENT_ID_CONFIG,
                clientId);
    }

    public T clientRack(String clientRack) {
        return property(
                CommonClientConfigs.CLIENT_RACK_CONFIG,
                clientRack);
    }

    public T reconnectBackoffMs(long reconnectBackoffMs) {
        return property(
                CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                reconnectBackoffMs);
    }

    public T reconnectBackoffMaxMs(long reconnectBackoffMaxMs) {
        return property(
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                reconnectBackoffMaxMs);
    }

    public T retriesMin() {
        return retries(0);
    }

    public T retriesMax() {
        return retries(Integer.MAX_VALUE);
    }

    public T retries(int retries) {
        return property(CommonClientConfigs.RETRIES_CONFIG, retries);
    }

    public T retryBackoffMs(long retryBackoffMs) {
        return property(
                CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                retryBackoffMs);
    }

    public T metricSampleWindowMs(long metricSampleWindowMs) {
        return property(
                CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG,
                metricSampleWindowMs);
    }

    public T metricNumSamples(int metricNumSamples) {
        return property(
                CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG,
                metricNumSamples);
    }

    public T metricRecordingLevelInfo() {
        return metricRecordingLevel(RecordingLevel.INFO);
    }

    public T metricRecordingLevelDebug() {
        return metricRecordingLevel(RecordingLevel.DEBUG);
    }

    public T metricRecordingLevel(RecordingLevel recordingLevel) {
        return metricRecordingLevel(recordingLevel.toString());
    }

    public T metricRecordingLevel(String metricRecordingLevel) {
        return property(
                CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG,
                metricRecordingLevel);
    }

    public <R extends MetricsReporter> T metricReporterClasses(
            Class<R> metricReporterClasse) {
        return metricReporterClasses(metricReporterClasse.getName());
    }

    public <R extends MetricsReporter> T metricReporterClasses(
            List<Class<? extends R>> metricReporterClasses) {
        return metricReporterClasses(
                metricReporterClasses.stream().map(Class::getName).collect(Collectors.joining(",")));
    }

    public T metricReporterClasses(String metricReporterClasses) {
        return property(
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                metricReporterClasses);
    }

    public T securityProtocolDefault() {
        return securityProtocol(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
    }

    public T securityProtocolPlaintext() {
        return securityProtocol(SecurityProtocol.PLAINTEXT);
    }

    public T securityProtocolSsl() {
        return securityProtocol(SecurityProtocol.SSL);
    }

    public T securityProtocolSaslSsl() {
        return securityProtocol(SecurityProtocol.SASL_SSL);
    }

    public T securityProtocolSaslPlaintext() {
        return securityProtocol(SecurityProtocol.SASL_PLAINTEXT);
    }

    public T securityProtocol(SecurityProtocol securityProtocol) {
        return property(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                securityProtocol.name);
    }

    public T securityProtocol(String securityProtocol) {
        return property(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                securityProtocol);
    }

    public T connectionsMaxIdleMs(long connectionMaxIdleMs) {
        return property(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionMaxIdleMs);
    }

    public T requestTimeoutMs(int timeoutMs) {
        return property(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
    }

    public T groupIdRandom() {
        return groupId(UUID.randomUUID().toString());
    }

    public T groupIdRandomIfAbsent() {
        return propertyIfAbsent(
                CommonClientConfigs.GROUP_ID_CONFIG,
                () -> UUID.randomUUID().toString());
    }

    public T groupId(String groupId) {
        return property(
                CommonClientConfigs.GROUP_ID_CONFIG,
                groupId);
    }

    public T groupInstanceId(String groupInstanceId) {
        return property(
                CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG,
                groupInstanceId);
    }

    public T maxPollIntervalMs(int maxPollIntervalMs) {
        return property(
                CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG,
                maxPollIntervalMs);
    }

    public T rebalanceTimeoutMs(int rebalanceTimeoutMs) {
        return property(
                CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG,
                rebalanceTimeoutMs);
    }

    public T sessionTimeoutMs(int sessionTimeoutMs) {
        return property(
                CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG,
                sessionTimeoutMs);
    }

    public T heartbeatIntervalMs(int heartbeatIntervalMs) {
        return property(
                CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG,
                heartbeatIntervalMs);
    }

    public T defaultApiTimeoutMs(int defaultApiTimeoutMs) {
        return property(
                CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG,
                defaultApiTimeoutMs);
    }

}
