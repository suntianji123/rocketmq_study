/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

/**
 * 默认的估算对象字节大小的估算器
 */
public final class DefaultMessageSizeEstimator implements MessageSizeEstimator {

    private static final class HandleImpl implements Handle {

        /**
         * 计算不出对象的大小时 返回的默认值 默认为8
         */
        private final int unknownSize;

        /**
         * 实例化估算对象字节大小的估算器
         * @param unknownSize 指定估算不出对象字节大小的默认字节值
         */
        private HandleImpl(int unknownSize) {
            this.unknownSize = unknownSize;
        }

        /**
         * 计算对象的字节大小
         * @param msg       需要计算字节大小的msg对象
         * @return
         */
        @Override
        public int size(Object msg) {
            if (msg instanceof ByteBuf) {//如果对象是ByteBuf类型 返回可读字节数
                return ((ByteBuf) msg).readableBytes();
            }
            if (msg instanceof ByteBufHolder) {//如果对象是ByteBufHolder类型  返回可读字节数
                return ((ByteBufHolder) msg).content().readableBytes();
            }
            if (msg instanceof FileRegion) {//如果是文件类型 返回0
                return 0;
            }

            //其他类型 返回默认值
            return unknownSize;
        }
    }

    /**
     * Return the default implementation which returns {@code 8} for unknown messages.
     */
    public static final MessageSizeEstimator DEFAULT = new DefaultMessageSizeEstimator(8);

    private final Handle handle;

    /**
     * Create a new instance
     *
     * @param unknownSize       The size which is returned for unknown messages.
     */
    public DefaultMessageSizeEstimator(int unknownSize) {
        if (unknownSize < 0) {
            throw new IllegalArgumentException("unknownSize: " + unknownSize + " (expected: >= 0)");
        }
        handle = new HandleImpl(unknownSize);
    }

    @Override
    public Handle newHandle() {
        return handle;
    }
}
