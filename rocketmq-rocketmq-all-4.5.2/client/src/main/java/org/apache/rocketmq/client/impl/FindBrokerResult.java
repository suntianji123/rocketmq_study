/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl;

/**
 * 查找广播站结果类
 */
public class FindBrokerResult {
    /**
     * 广播站地址
     */
    private final String brokerAddr;

    /**
     * 广播站是否为从站
     */
    private final boolean slave;

    /**
     * 广播站版本
     */
    private final int brokerVersion;

    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = 0;
    }

    /**
     * 实例化一个查找广播站结果对象
     * @param brokerAddr 广播站地址
     * @param slave 是否为从站
     * @param brokerVersion 广播站版本
     */
    public FindBrokerResult(String brokerAddr, boolean slave, int brokerVersion) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = brokerVersion;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public boolean isSlave() {
        return slave;
    }

    public int getBrokerVersion() {
        return brokerVersion;
    }
}
