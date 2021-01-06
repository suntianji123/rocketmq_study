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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 响应的异步操作类
 */
public class ResponseFuture {

    /**
     * 命令id
     */
    private final int opaque;

    /**
     * 与远程服务器建立的channel连接
     */
    private final Channel processChannel;

    /**
     * 执行命令超时时间
     */
    private final long timeoutMillis;

    /**
     * 执行完命令后回调方法
     */
    private final InvokeCallback invokeCallback;

    /**
     * 执行命令开始时间
     */
    private final long beginTimestamp = System.currentTimeMillis();

    /**
     * 计数器锁对象
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final SemaphoreReleaseOnlyOnce once;

    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    /**
     * 服务器返回的远程命令对象
     */
    private volatile RemotingCommand responseCommand;

    /**
     * 是否向远程服务器发送请求成功
     */
    private volatile boolean sendRequestOK = true;

    /**
     * 响应失败的原因
     */
    private volatile Throwable cause;

    /**
     * 实例化一个响应的异步操作对象
     * @param channel 与远程服务器建立的channel对象
     * @param opaque 远程命令id
     * @param timeoutMillis 执行远程命令超时时间
     * @param invokeCallback 收到响应之后的回调方法对象
     * @param once
     */
    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback,
        SemaphoreReleaseOnlyOnce once) {
        //设置命令id
        this.opaque = opaque;
        //设置与远程服务器建立的channel连接
        this.processChannel = channel;

        //设置等待响应的超时时间
        this.timeoutMillis = timeoutMillis;
        //设置执行命令回调
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand
            + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause
            + ", opaque=" + opaque
            + ", processChannel=" + processChannel
            + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback
            + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
