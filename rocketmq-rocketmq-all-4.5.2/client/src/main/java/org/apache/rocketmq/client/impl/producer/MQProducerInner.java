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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

/**
 * 生产者接口
 */
public interface MQProducerInner {
    Set<String> getPublishTopicList();

    /**
     * 判断生产者发布的某个主题的发布路径信息是否需要更新
     * @param topic 主题
     * @return 这个生产者维护的主题发布路径信息为null或者主题消息队列列表为空
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();
    TransactionListener getCheckListener();

    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新生产者组的某个主题的主题发布信息
     * @param topic 主题
     * @param info 主题发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    boolean isUnitMode();
}
