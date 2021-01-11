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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

/**
 * netty channel事件监听器接口
 */
public interface ChannelEventListener {

    /**
     * channel连接上了远程服务器
     * @param remoteAddr 远程服务器地址
     * @param channel channel对象
     */
    void onChannelConnect(final String remoteAddr, final Channel channel);

    /**
     * channel与远程nettty server断开连接
     * @param remoteAddr 远程服务器地址
     * @param channel channel连接对象
     */
    void onChannelClose(final String remoteAddr, final Channel channel);

    /**
     * channel处理io事件发生异常
     * @param remoteAddr 远程服务器地址
     * @param channel channel连接对象
     */
    void onChannelException(final String remoteAddr, final Channel channel);

    /**
     * channel长时间没哟远程服务器发生io事件
     * @param remoteAddr 远程服务器地址
     * @param channel channel连接对象
     */
    void onChannelIdle(final String remoteAddr, final Channel channel);
}
