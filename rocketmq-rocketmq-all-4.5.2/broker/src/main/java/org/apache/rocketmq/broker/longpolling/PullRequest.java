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
package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageFilter;

/**
 * 广播站缓存的消费者从广播站消费队列拉取消息请求类
 */
public class PullRequest {

    /**
     * 消费者发起的拉取消息的请求体
     */
    private final RemotingCommand requestCommand;

    /**
     * 消费者与广播站建立的channel连接
     */
    private final Channel clientChannel;

    /**
     * 悬挂请求的最长时间
     */
    private final long timeoutMillis;

    /**
     * 悬挂请求的开始时间
     */
    private final long suspendTimestamp;

    /**
     * 消费者拉取消息的在消费队列中偏移量
     */
    private final long pullFromThisOffset;

    /**
     * 消费者组主题订阅数据
     */
    private final SubscriptionData subscriptionData;

    /**
     * 消费者组主题消息过滤器
     */
    private final MessageFilter messageFilter;

    /**
     * 实例化一个拉取请求对象
     * @param requestCommand 消费者请求体
     * @param clientChannel 消费者通道连接
     * @param timeoutMillis 悬挂的最长时间
     * @param suspendTimestamp 开始悬挂的时间
     * @param pullFromThisOffset 从消费队列拉取消息的偏移量
     * @param subscriptionData 主题消息订阅数据
     * @param messageFilter 消息过滤器
     */
    public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis, long suspendTimestamp,
        long pullFromThisOffset, SubscriptionData subscriptionData,
        MessageFilter messageFilter) {
        //设置拉取消息的请求
        this.requestCommand = requestCommand;
        //设置与消费者建立的channel连接
        this.clientChannel = clientChannel;
        //设置悬挂的超时时间
        this.timeoutMillis = timeoutMillis;
        //设置悬挂的起始时间
        this.suspendTimestamp = suspendTimestamp;
        //设置消费者拉取消息在消费者队列中的起始偏移量
        this.pullFromThisOffset = pullFromThisOffset;
        //设置主题订阅数据
        this.subscriptionData = subscriptionData;
        //设置消息过滤器
        this.messageFilter = messageFilter;
    }

    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
