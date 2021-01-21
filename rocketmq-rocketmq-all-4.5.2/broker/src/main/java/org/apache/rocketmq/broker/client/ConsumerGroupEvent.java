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
package org.apache.rocketmq.broker.client;

/**
 * 消费者组时间
 */
public enum ConsumerGroupEvent {

    /**
     * 广播站所维护的消费者组的成员变更 或者消费者组的订阅数据发生变更
     */
    CHANGE,
    /**
     * The group of consumer is unregistered.
     */
    UNREGISTER,

    /**
     * 注册消费者组的订阅配置
     */
    REGISTER
}
