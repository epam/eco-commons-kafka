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
package com.epam.eco.commons.kafka;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.security.JaasUtils;

import kafka.utils.ZkUtils;

/**
 * @author Andrei_Tytsik
 */
@Deprecated
public abstract class ZkCaller {

    private static final int DEFAULT_SESSION_TIMEOUT = 10000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

    private ZkCaller() {
    }

    public static <R> R call(String zookeeperConnect, ZkCallable<R> zkCallable) {
        return ZkCaller.call(
                zookeeperConnect,
                DEFAULT_SESSION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT,
                zkCallable);
    }

    public static <R> R call(
            String zookeeperConnect,
            int sessionTimeout,
            int connectionTimeout,
            ZkCallable<R> zkCallable) {
        Validate.notBlank(zookeeperConnect, "ZK connect is blank");
        Validate.isTrue(sessionTimeout > 0, "Session timeout is invalid");
        Validate.isTrue(connectionTimeout > 0, "Connection timeout is invalid");
        Validate.notNull(zkCallable, "Callable is null");

        ZkUtils zkUtils = ZkCaller.initZkUtils(zookeeperConnect, sessionTimeout, connectionTimeout);
        try {
            return zkCallable.call(zkUtils);
        } catch (Exception ex) {
            throw new RuntimeException("Zk call failed", ex);
        } finally {
            closeZkUtils(zkUtils);
        }
    }

    private static ZkUtils initZkUtils(
            String zookeeperConnect,
            int sessionTimeout,
            int connectionTimeout) {
        return ZkUtils.apply(
                zookeeperConnect,
                sessionTimeout,
                connectionTimeout,
                JaasUtils.isZkSecurityEnabled());
    }

    private static void closeZkUtils(ZkUtils zkUtils) {
        if (zkUtils != null) {
            zkUtils.close();
        }
    }

    public static interface ZkCallable<R> {
        public R call(ZkUtils zkUtils) throws Exception;
    }

}
