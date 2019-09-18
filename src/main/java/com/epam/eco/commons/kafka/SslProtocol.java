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

/**
 * @author Andrei_Tytsik
 */
public enum SslProtocol {

    TLS("TLS"),
    TLS_V1_1("TLSv1.1"),
    TLS_V1_2("TLSv1.2"),
    SSL("SSL"),
    SSL_V2("SSLv2"),
    SSL_V3("SSLv3");

    public final String name;

    SslProtocol(String name) {
        this.name = name;
    }

}
