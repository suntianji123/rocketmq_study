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

package io.openmessaging.storage.dledger;

/**
 * 向远程节点添加消息的异步操作类
 * @param <T> 异步操作等待的结果类型
 */
public class AppendFuture<T> extends TimeoutFuture<T> {

    /**
     * 消息在commitlog文件系统中的偏移量
     */
    private long pos = -1;

    public AppendFuture() {

    }

    /**
     * 实例化一个向远程节点添加消息的异步操作对象
     * @param timeOutMs 异步操作超时时间
     */
    public AppendFuture(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public static <T> AppendFuture<T> newCompletedFuture(long pos, T value) {
        AppendFuture<T> future = new AppendFuture<T>();
        future.setPos(pos);
        future.complete(value);
        return future;
    }
}
