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

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 缓存的请求任务
 */
public class RequestTask implements Runnable {
    /**
     * run方法体
     */
    private final Runnable runnable;
    /**
     * 创建时间
     */
    private final long createTimestamp = System.currentTimeMillis();

    /**
     * 请求的来源发起channel
     */
    private final Channel channel;

    /**
     * 请求体
     */
    private final RemotingCommand request;

    /**
     * 请求任务是否已经停止
     */
    private boolean stopRun = false;

    /**
     * 实例化一个请求任务
     * @param runnable run方法体
     * @param channel 请求的channel
     * @param request 请求对象
     */
    public RequestTask(final Runnable runnable, final Channel channel, final RemotingCommand request) {
        //设置run方法体
        this.runnable = runnable;
        //设置channel
        this.channel = channel;
        //设置请求体
        this.request = request;
    }

    @Override
    public int hashCode() {
        int result = runnable != null ? runnable.hashCode() : 0;
        result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (isStopRun() ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RequestTask))
            return false;

        final RequestTask that = (RequestTask) o;

        if (getCreateTimestamp() != that.getCreateTimestamp())
            return false;
        if (isStopRun() != that.isStopRun())
            return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null)
            return false;
        return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean isStopRun() {
        return stopRun;
    }

    public void setStopRun(final boolean stopRun) {
        this.stopRun = stopRun;
    }

    @Override
    public void run() {
        if (!this.stopRun)
            this.runnable.run();
    }

    public void returnResponse(int code, String remark) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
        response.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(response);
    }
}
