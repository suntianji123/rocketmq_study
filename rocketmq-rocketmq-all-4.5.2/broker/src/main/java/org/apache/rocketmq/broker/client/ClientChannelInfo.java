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

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

/**
 * 实例化一个客户端通道信息类
 */
public class ClientChannelInfo {
    /**
     * 与生产者、消费者建立的channel连接
     */
    private final Channel channel;

    /**
     * 客户端id
     */
    private final String clientId;

    /**
     * 语言
     */
    private final LanguageCode language;

    /**
     * rocketmq版本
     */
    private final int version;

    /**
     * 上一次更新时间（刚被实例化的时间为当前时间）
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(Channel channel) {
        this(channel, null, null, 0);
    }

    /**
     * 实例化客户端channel信息对象
     * @param channel 客户端channel
     * @param clientId 客户端id
     * @param language 客户端语言
     * @param version  客户端版本
     */
    public ClientChannelInfo(Channel channel, String clientId, LanguageCode language, int version) {
        //设置channel
        this.channel = channel;
        //设置客户端id
        this.clientId = clientId;
        //设置客户端语言
        this.language = language;
        //设置客户端版本
        this.version = version;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getClientId() {
        return clientId;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public int getVersion() {
        return version;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result + ((language == null) ? 0 : language.hashCode());
        result = prime * result + (int) (lastUpdateTimestamp ^ (lastUpdateTimestamp >>> 32));
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClientChannelInfo other = (ClientChannelInfo) obj;
        if (channel == null) {
            if (other.channel != null)
                return false;
        } else if (this.channel != other.channel) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ClientChannelInfo [channel=" + channel + ", clientId=" + clientId + ", language=" + language
            + ", version=" + version + ", lastUpdateTimestamp=" + lastUpdateTimestamp + "]";
    }
}
