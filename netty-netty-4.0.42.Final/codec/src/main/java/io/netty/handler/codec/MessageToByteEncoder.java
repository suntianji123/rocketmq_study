/*
 * Copyright 2012 The Netty Project
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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.TypeParameterMatcher;


/**
 * 消息对象I 转为ByteBuf对象
 * @param <I> 消息类型
 */
public abstract class MessageToByteEncoder<I> extends ChannelOutboundHandlerAdapter {

    /**
     * I类型匹配器
     */
    private final TypeParameterMatcher matcher;

    /**
     * 是否优先使用堆外内存空间分配缓冲区
     */
    private final boolean preferDirect;

    /**
     * @see {@link #MessageToByteEncoder(boolean)} with {@code true} as boolean parameter.
     */
    protected MessageToByteEncoder() {
        this(true);
    }

    /**
     * @see {@link #MessageToByteEncoder(Class, boolean)} with {@code true} as boolean value.
     */
    protected MessageToByteEncoder(Class<? extends I> outboundMessageType) {
        this(outboundMessageType, true);
    }

    /**
     * Create a new instance which will try to detect the types to match out of the type parameter of the class.
     *
     * @param preferDirect          {@code true} if a direct {@link ByteBuf} should be tried to be used as target for
     *                              the encoded messages. If {@code false} is used it will allocate a heap
     *                              {@link ByteBuf}, which is backed by an byte array.
     */
    protected MessageToByteEncoder(boolean preferDirect) {
        matcher = TypeParameterMatcher.find(this, MessageToByteEncoder.class, "I");
        this.preferDirect = preferDirect;
    }

    /**
     * Create a new instance
     *
     * @param outboundMessageType   The tpye of messages to match
     * @param preferDirect          {@code true} if a direct {@link ByteBuf} should be tried to be used as target for
     *                              the encoded messages. If {@code false} is used it will allocate a heap
     *                              {@link ByteBuf}, which is backed by an byte array.
     */
    protected MessageToByteEncoder(Class<? extends I> outboundMessageType, boolean preferDirect) {
        matcher = TypeParameterMatcher.get(outboundMessageType);
        this.preferDirect = preferDirect;
    }

    /**
     * Returns {@code true} if the given message should be handled. If {@code false} it will be passed to the next
     * {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     */
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return matcher.match(msg);
    }

    /**
     * 向channel的输出缓冲区写入数据对象
     * @param ctx channelhandlerContext对象
     * @param msg 数据对象
     * @param promise 写入的异步操作对象
     * @throws Exception
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        //定义一个ByteBuf对象
        ByteBuf buf = null;
        try {
            if (acceptOutboundMessage(msg)) {//判断msg是否为泛型I指定的类型
                //将msg转为指定泛型I的类型
                @SuppressWarnings("unchecked")
                I cast = (I) msg;
                //使用堆外内存分配一个缓冲区对象
                buf = allocateBuffer(ctx, cast, preferDirect);
                try {
                    //编码 将msg转为字节数组 写入ByteBuf对象
                    encode(ctx, cast, buf);
                } finally {
                    //释放当前线程对消息对象的引用
                    ReferenceCountUtil.release(cast);
                }

                if (buf.isReadable()) {//如果分配的ByteBuf可读
                    //向channel写入ByteBuf对象
                    ctx.write(buf, promise);
                } else {
                    //释放ByteBuf
                    buf.release();
                    //向channel中写入空的ByteBuffewr
                    ctx.write(Unpooled.EMPTY_BUFFER, promise);
                }
                buf = null;
            } else {
                ctx.write(msg, promise);
            }
        } catch (EncoderException e) {
            throw e;
        } catch (Throwable e) {
            throw new EncoderException(e);
        } finally {
            if (buf != null) {//释放ByteBuf
                buf.release();
            }
        }
    }

    /**
     * Allocate a {@link ByteBuf} which will be used as argument of {@link #encode(ChannelHandlerContext, I, ByteBuf)}.
     * Sub-classes may override this method to returna {@link ByteBuf} with a perfect matching {@code initialCapacity}.
     */
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, @SuppressWarnings("unused") I msg,
                               boolean preferDirect) throws Exception {
        if (preferDirect) {
            return ctx.alloc().ioBuffer();
        } else {
            return ctx.alloc().heapBuffer();
        }
    }

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg           the message to encode
     * @param out           the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception    is thrown if an error accour
     */
    protected abstract void encode(ChannelHandlerContext ctx, I msg, ByteBuf out) throws Exception;
}
