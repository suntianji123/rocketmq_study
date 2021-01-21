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
 * 某个消费者组消费者成员或者消费者组订阅发生变更的事件处理接口
 */
public interface ConsumerIdsChangeListener {

    /**
     * 事件处理
     * @param event 消费者组发生的事件类型
     * @param group 消费者组名
     * @param args 事件的处理参数
     */
    void handle(ConsumerGroupEvent event, String group, Object... args);
}
