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
package io.netty.channel;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ThreadLocalRandom;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * A skeletal {@link Channel} implementation.
 */
public abstract class AbstractChannel extends DefaultAttributeMap implements Channel {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractChannel.class);

    private static final ClosedChannelException FLUSH0_CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), AbstractUnsafe.class, "flush0()");
    private static final ClosedChannelException ENSURE_OPEN_CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), AbstractUnsafe.class, "ensureOpen(...)");
    private static final ClosedChannelException CLOSE_CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), AbstractUnsafe.class, "close(...)");
    private static final ClosedChannelException WRITE_CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), AbstractUnsafe.class, "write(...)");
    private static final NotYetConnectedException FLUSH0_NOT_YET_CONNECTED_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new NotYetConnectedException(), AbstractUnsafe.class, "flush0()");

    private final Channel parent;
    private final long hashCode = ThreadLocalRandom.current().nextLong();
    private final Unsafe unsafe;
    private final DefaultChannelPipeline pipeline;
    private final ChannelFuture succeededFuture = new SucceededChannelFuture(this, null);
    private final VoidChannelPromise voidPromise = new VoidChannelPromise(this, true);
    private final VoidChannelPromise unsafeVoidPromise = new VoidChannelPromise(this, false);
    private final CloseFuture closeFuture = new CloseFuture(this);

    /**
     * 本地服务地址
     */
    private volatile SocketAddress localAddress;

    /**
     * jdk底层channel连接的远程服务器地址
     */
    private volatile SocketAddress remoteAddress;

    /**
     * channel绑定的轮训器的执行器
     */
    private volatile EventLoop eventLoop;

    /**
     * 是否已经注册过
     */
    private volatile boolean registered;

    /** Cache for the string representation of this channel */
    private boolean strValActive;
    private String strVal;

    /**
     * Creates a new instance.
     *
     * @param parent
     *        the parent of this channel. {@code null} if there's no parent.
     */
    protected AbstractChannel(Channel parent) {
        this.parent = parent;
        unsafe = newUnsafe();
        pipeline = newChannelPipeline();
    }

    /**
     * Returns a new {@link DefaultChannelPipeline} instance.
     */
    protected DefaultChannelPipeline newChannelPipeline() {
        return new DefaultChannelPipeline(this);
    }

    @Override
    public boolean isWritable() {
        ChannelOutboundBuffer buf = unsafe.outboundBuffer();
        return buf != null && buf.isWritable();
    }

    @Override
    public Channel parent() {
        return parent;
    }

    @Override
    public ChannelPipeline pipeline() {
        return pipeline;
    }

    @Override
    public ByteBufAllocator alloc() {
        return config().getAllocator();
    }

    @Override
    public EventLoop eventLoop() {
        EventLoop eventLoop = this.eventLoop;
        if (eventLoop == null) {
            throw new IllegalStateException("channel not registered to an event loop");
        }
        return eventLoop;
    }

    @Override
    public SocketAddress localAddress() {
        SocketAddress localAddress = this.localAddress;
        if (localAddress == null) {
            try {
                this.localAddress = localAddress = unsafe().localAddress();
            } catch (Throwable t) {
                // Sometimes fails on a closed socket in Windows.
                return null;
            }
        }
        return localAddress;
    }

    /**
     * @deprecated no use-case for this.
     */
    @Deprecated
    protected void invalidateLocalAddress() {
        localAddress = null;
    }

    @Override
    public SocketAddress remoteAddress() {
        SocketAddress remoteAddress = this.remoteAddress;
        if (remoteAddress == null) {
            try {
                this.remoteAddress = remoteAddress = unsafe().remoteAddress();
            } catch (Throwable t) {
                // Sometimes fails on a closed socket in Windows.
                return null;
            }
        }
        return remoteAddress;
    }

    /**
     * @deprecated no use-case for this.
     */
    @Deprecated
    protected void invalidateRemoteAddress() {
        remoteAddress = null;
    }

    @Override
    public boolean isRegistered() {
        return registered;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return pipeline.bind(localAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return pipeline.connect(remoteAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return pipeline.connect(remoteAddress, localAddress);
    }

    @Override
    public ChannelFuture disconnect() {
        return pipeline.disconnect();
    }

    @Override
    public ChannelFuture close() {
        return pipeline.close();
    }

    @Override
    public ChannelFuture deregister() {
        return pipeline.deregister();
    }

    @Override
    public Channel flush() {
        pipeline.flush();
        return this;
    }

    /**
     * 绑定端口
     * @param localAddress 本地地址
     * @param promise 绑定端口的异步操作对象
     * @return
     */
    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        //调用的pipeline的bind方法
        return pipeline.bind(localAddress, promise);
    }

    /**
     * 连接远程服务器
     * @param remoteAddress 远程服务器地址对象
     * @param promise 连接的异步操作对象
     * @return
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        //调用pipeline的连接方法
        return pipeline.connect(remoteAddress, promise);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return pipeline.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return pipeline.disconnect(promise);
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return pipeline.close(promise);
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return pipeline.deregister(promise);
    }

    @Override
    public Channel read() {
        pipeline.read();
        return this;
    }

    @Override
    public ChannelFuture write(Object msg) {
        return pipeline.write(msg);
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        return pipeline.write(msg, promise);
    }

    /**
     * 向channel中写入数据
     * @param msg 写入数据对象
     * @return
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return pipeline.writeAndFlush(msg);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        //test for ok
        return pipeline.writeAndFlush(msg, promise);
    }

    @Override
    public ChannelPromise newPromise() {
        return new DefaultChannelPromise(this);
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return new DefaultChannelProgressivePromise(this);
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return succeededFuture;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return new FailedChannelFuture(this, null, cause);
    }

    @Override
    public ChannelFuture closeFuture() {
        return closeFuture;
    }

    @Override
    public Unsafe unsafe() {
        return unsafe;
    }

    /**
     * Create a new {@link AbstractUnsafe} instance which will be used for the life-time of the {@link Channel}
     */
    protected abstract AbstractUnsafe newUnsafe();

    /**
     * Returns the ID of this channel.
     */
    @Override
    public final int hashCode() {
        return (int) hashCode;
    }

    /**
     * Returns {@code true} if and only if the specified object is identical
     * with this channel (i.e: {@code this == o}).
     */
    @Override
    public final boolean equals(Object o) {
        return this == o;
    }

    @Override
    public final int compareTo(Channel o) {
        if (this == o) {
            return 0;
        }

        long ret = hashCode - o.hashCode();
        if (ret > 0) {
            return 1;
        }
        if (ret < 0) {
            return -1;
        }

        ret = System.identityHashCode(this) - System.identityHashCode(o);
        if (ret != 0) {
            return (int) ret;
        }

        // Jackpot! - different objects with same hashes
        throw new Error();
    }

    /**
     * Returns the {@link String} representation of this channel.  The returned
     * string contains the {@linkplain #hashCode() ID}, {@linkplain #localAddress() local address},
     * and {@linkplain #remoteAddress() remote address} of this channel for
     * easier identification.
     */
    @Override
    public String toString() {
        boolean active = isActive();
        if (strValActive == active && strVal != null) {
            return strVal;
        }

        SocketAddress remoteAddr = remoteAddress();
        SocketAddress localAddr = localAddress();
        if (remoteAddr != null) {
            String activeNotation = active? "-" : "!";
            strVal = String.format("[id: 0x%08x, L:%s %s R:%s]", (int) hashCode, localAddr, activeNotation, remoteAddr);
        } else if (localAddr != null) {
            strVal = String.format("[id: 0x%08x, L:%s]", (int) hashCode, localAddr);
        } else {
            strVal = String.format("[id: 0x%08x]", (int) hashCode);
        }

        strValActive = active;
        return strVal;
    }

    @Override
    public final ChannelPromise voidPromise() {
        return voidPromise;
    }

    /**
     * {@link Unsafe} implementation which sub-classes must extend and use.
     */
    protected abstract class AbstractUnsafe implements Unsafe {

        private volatile ChannelOutboundBuffer outboundBuffer = new ChannelOutboundBuffer(AbstractChannel.this);

        /**
         * 是否正在刷新输出缓冲区
         */
        private boolean inFlush0;

        /**
         * channel是否一次都没有注册到某个eventloop上
         */
        private boolean neverRegistered = true;

        private void assertEventLoop() {
            assert !registered || eventLoop.inEventLoop();
        }

        @Override
        public final ChannelOutboundBuffer outboundBuffer() {
            return outboundBuffer;
        }

        @Override
        public final SocketAddress localAddress() {
            return localAddress0();
        }

        @Override
        public final SocketAddress remoteAddress() {
            return remoteAddress0();
        }

        /**
         * 将当前channel注册到某个eventloop执行器上
         * @param eventLoop channel将要被注册到的eventloop执行器
         * @param promise 异步操偶作对象
         */
        @Override
        public final void register(EventLoop eventLoop, final ChannelPromise promise) {
            if (eventLoop == null) {//执行器不能为空
                throw new NullPointerException("eventLoop");
            }
            //如果channel已经被注册过了
            if (isRegistered()) {
                //设置注册失败的原因 channel已经注册过一次了
                promise.setFailure(new IllegalStateException("registered to an event loop already"));
                return;
            }

            //如果将要注册到的执行器不适用于当前channel
            if (!isCompatible(eventLoop)) {
                //设置注册失败原因 当前channel不能注册到这种类型的eventloop上
                promise.setFailure(
                        new IllegalStateException("incompatible event loop type: " + eventLoop.getClass().getName()));
                return;
            }

            //设置轮训当前channel事件的执行器
            AbstractChannel.this.eventLoop = eventLoop;

            //如果当前线程是执行器的工作线程 直接调用register方法进行注册
            if (eventLoop.inEventLoop()) {
                register0(promise);
            } else {
                try {
                    //当前线程不是执行器的工作线程 向执行器添加一个注册任务
                    eventLoop.execute(new Runnable() {
                        @Override
                        public void run() {
                            register0(promise);
                        }
                    });
                } catch (Throwable t) {
                    logger.warn(
                            "Force-closing a channel whose registration task was not accepted by an event loop: {}",
                            AbstractChannel.this, t);
                    closeForcibly();
                    closeFuture.setClosed();
                    safeSetFailure(promise, t);
                }
            }
        }

        /**
         * 将当前channel注册到eventlooop的执行方法体
         * @param promise 注册的异步操作对象
         */
        private void register0(ChannelPromise promise) {
            try {

                //注册的异步操作对象设置不能取消 并且通道已经打开
                if (!promise.setUncancellable() || !ensureOpen(promise)) {
                    return;
                }
                //第一次注册标识
                boolean firstRegistration = neverRegistered;
                //开始注册 将channel底层的selectionkey注册到eventloop的selector轮训器上
                doRegister();
                //设置从没有注册为false
                neverRegistered = false;
                //设置channel已经注册到eventloop的标志位true
                registered = true;

                //执行pipeline在channel没有注册到eventloop之前添加到channelhandlerContext链表上的handler的handleradded方法
                pipeline.invokeHandlerAddedIfNeeded();

                //设置注册的异步操作结果为成功
                safeSetSuccess(promise);

                //下发channel注册事件 交由每一个handler的channelregistered方法进行处理
                pipeline.fireChannelRegistered();

                //如果通道已经激活
                if (isActive()) {
                    if (firstRegistration) {//如果是第一次注册
                        //下发channel激活事件
                        pipeline.fireChannelActive();
                    } else if (config().isAutoRead()) {
                        // This channel was registered before and autoRead() is set. This means we need to begin read
                        // again so that we process inbound data.
                        //
                        // See https://github.com/netty/netty/issues/4805
                        //开始读取数据
                        beginRead();
                    }
                }
            } catch (Throwable t) {
                // Close the channel directly to avoid FD leak.
                closeForcibly();
                closeFuture.setClosed();
                safeSetFailure(promise, t);
            }
        }

        /**
         * 绑定端口
         * @param localAddress 端口地址
         * @param promise 绑定端口的异步操作对象
         */
        @Override
        public final void bind(final SocketAddress localAddress, final ChannelPromise promise) {
            assertEventLoop();

            //channel必须打开 绑定端口的异步操作设置不能取消
            if (!promise.setUncancellable() || !ensureOpen(promise)) {
                return;
            }

            // See: https://github.com/netty/netty/issues/576
            if (Boolean.TRUE.equals(config().getOption(ChannelOption.SO_BROADCAST)) &&
                localAddress instanceof InetSocketAddress &&
                !((InetSocketAddress) localAddress).getAddress().isAnyLocalAddress() &&
                !PlatformDependent.isWindows() && !PlatformDependent.isRoot()) {
                // Warn a user about the fact that a non-root user can't receive a
                // broadcast packet on *nix if the socket is bound on non-wildcard address.
                logger.warn(
                        "A non-root user can't receive a broadcast packet if the socket " +
                        "is not bound to a wildcard address; binding to a non-wildcard " +
                        "address (" + localAddress + ") anyway as requested.");
            }

            //channel的激活状天天
            boolean wasActive = isActive();
            try {
                //执行绑定
                doBind(localAddress);
            } catch (Throwable t) {
                //设置异步操作失败
                safeSetFailure(promise, t);
                //关闭 channel
                closeIfClosed();
                return;
            }

            //之前channel没有激活 现在激活了 下发channel激活是哪
            if (!wasActive && isActive()) {
                invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelActive();
                    }
                });
            }

            //设置绑定端口的异步操作成功
            safeSetSuccess(promise);
        }

        @Override
        public final void disconnect(final ChannelPromise promise) {
            assertEventLoop();

            if (!promise.setUncancellable()) {
                return;
            }

            boolean wasActive = isActive();
            try {
                doDisconnect();
            } catch (Throwable t) {
                safeSetFailure(promise, t);
                closeIfClosed();
                return;
            }

            if (wasActive && !isActive()) {
                invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelInactive();
                    }
                });
            }

            safeSetSuccess(promise);
            closeIfClosed(); // doDisconnect() might have closed the channel
        }

        @Override
        public final void close(final ChannelPromise promise) {
            assertEventLoop();

            close(promise, CLOSE_CLOSED_CHANNEL_EXCEPTION, CLOSE_CLOSED_CHANNEL_EXCEPTION, false);
        }

        private void close(final ChannelPromise promise, final Throwable cause,
                           final ClosedChannelException closeCause, final boolean notify) {
            if (!promise.setUncancellable()) {
                return;
            }

            final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
            if (outboundBuffer == null) {
                // Only needed if no VoidChannelPromise.
                if (!(promise instanceof VoidChannelPromise)) {
                    // This means close() was called before so we just register a listener and return
                    closeFuture.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            promise.setSuccess();
                        }
                    });
                }
                return;
            }

            if (closeFuture.isDone()) {
                // Closed already.
                safeSetSuccess(promise);
                return;
            }

            final boolean wasActive = isActive();
            this.outboundBuffer = null; // Disallow adding any messages and flushes to outboundBuffer.
            Executor closeExecutor = prepareToClose();
            if (closeExecutor != null) {
                closeExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // Execute the close.
                            doClose0(promise);
                        } finally {
                            // Call invokeLater so closeAndDeregister is executed in the EventLoop again!
                            invokeLater(new Runnable() {
                                @Override
                                public void run() {
                                    // Fail all the queued messages
                                    outboundBuffer.failFlushed(cause, notify);
                                    outboundBuffer.close(closeCause);
                                    fireChannelInactiveAndDeregister(wasActive);
                                }
                            });
                        }
                    }
                });
            } else {
                try {
                    // Close the channel and fail the queued messages in all cases.
                    doClose0(promise);
                } finally {
                    // Fail all the queued messages.
                    outboundBuffer.failFlushed(cause, notify);
                    outboundBuffer.close(closeCause);
                }
                if (inFlush0) {
                    invokeLater(new Runnable() {
                        @Override
                        public void run() {
                            fireChannelInactiveAndDeregister(wasActive);
                        }
                    });
                } else {
                    fireChannelInactiveAndDeregister(wasActive);
                }
            }
        }

        private void doClose0(ChannelPromise promise) {
            try {
                doClose();
                closeFuture.setClosed();
                safeSetSuccess(promise);
            } catch (Throwable t) {
                closeFuture.setClosed();
                safeSetFailure(promise, t);
            }
        }

        private void fireChannelInactiveAndDeregister(final boolean wasActive) {
            deregister(voidPromise(), wasActive && !isActive());
        }

        @Override
        public final void closeForcibly() {
            assertEventLoop();

            try {
                doClose();
            } catch (Exception e) {
                logger.warn("Failed to close a channel.", e);
            }
        }

        @Override
        public final void deregister(final ChannelPromise promise) {
            assertEventLoop();

            deregister(promise, false);
        }

        private void deregister(final ChannelPromise promise, final boolean fireChannelInactive) {
            if (!promise.setUncancellable()) {
                return;
            }

            if (!registered) {
                safeSetSuccess(promise);
                return;
            }

            // As a user may call deregister() from within any method while doing processing in the ChannelPipeline,
            // we need to ensure we do the actual deregister operation later. This is needed as for example,
            // we may be in the ByteToMessageDecoder.callDecode(...) method and so still try to do processing in
            // the old EventLoop while the user already registered the Channel to a new EventLoop. Without delay,
            // the deregister operation this could lead to have a handler invoked by different EventLoop and so
            // threads.
            //
            // See:
            // https://github.com/netty/netty/issues/4435
            invokeLater(new Runnable() {
                @Override
                public void run() {
                    try {
                        doDeregister();
                    } catch (Throwable t) {
                        logger.warn("Unexpected exception occurred while deregistering a channel.", t);
                    } finally {
                        if (fireChannelInactive) {
                            pipeline.fireChannelInactive();
                        }
                        // Some transports like local and AIO does not allow the deregistration of
                        // an open channel.  Their doDeregister() calls close(). Consequently,
                        // close() calls deregister() again - no need to fire channelUnregistered, so check
                        // if it was registered.
                        if (registered) {
                            registered = false;
                            pipeline.fireChannelUnregistered();
                        }
                        safeSetSuccess(promise);
                    }
                }
            });
        }

        @Override
        public final void beginRead() {
            assertEventLoop();

            if (!isActive()) {
                return;
            }

            try {
                doBeginRead();
            } catch (final Exception e) {
                invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireExceptionCaught(e);
                    }
                });
                close(voidPromise());
            }
        }

        /**
         * 向channel中写入数据
         * @param msg 消息对象
         * @param promise 写入的异步操作
         */
        @Override
        public final void write(Object msg, ChannelPromise promise) {
            assertEventLoop();

            //获取输出缓冲区对象
            ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
            if (outboundBuffer == null) {//输出缓冲区为空
                // If the outboundBuffer is null we know the channel was closed and so
                // need to fail the future right away. If it is not null the handling of the rest
                // will be done in flush0()
                // See https://github.com/netty/netty/issues/2362
                //设置向channel写入数据的异步操作结果为失败
                safeSetFailure(promise, WRITE_CLOSED_CHANNEL_EXCEPTION);
                // release message now to prevent resource-leak
                //释放消息
                ReferenceCountUtil.release(msg);
                return;
            }

            //消息的大小
            int size;
            try {
                //过滤到不是ByteBuf类型的消息 并且如果msg 不是堆外ByteBuf 将数据复制到堆外ByteBuf返回
                msg = filterOutboundMessage(msg);

                //计算消息的大小
                size = pipeline.estimatorHandle().size(msg);
                if (size < 0) {
                    size = 0;
                }
            } catch (Throwable t) {
                safeSetFailure(promise, t);
                ReferenceCountUtil.release(msg);
                return;
            }

            //向channel的输出缓冲区添加一个消息
            outboundBuffer.addMessage(msg, size, promise);
        }

        /**
         * 刷新channel的输出缓冲区
         */
        @Override
        public final void flush() {
            assertEventLoop();

            //获取channel输出缓冲区对象
            ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
            if (outboundBuffer == null) {//输出缓冲区为null  直接返回
                return;
            }

            //执行输出缓冲区的刷新操作 将entry添加到刷新链表
            outboundBuffer.addFlush();
            flush0();
        }

        /**
         * 刷新输出缓冲区
         */
        protected void flush0() {
            if (inFlush0) {//如果channel正在刷新输出缓冲区 直接返回
                // Avoid re-entrance
                return;
            }

            //获取输出缓冲区
            final ChannelOutboundBuffer outboundBuffer = this.outboundBuffer;
            if (outboundBuffer == null || outboundBuffer.isEmpty()) {//如果刷出缓冲区没有需要刷新的数据
                return;
            }

            //设置channel正在刷新输出缓冲区
            inFlush0 = true;

            // Mark all pending write requests as failure if the channel is inactive.
            if (!isActive()) {//如通channel没有激活
                try {
                    if (isOpen()) {//channel打开了
                        outboundBuffer.failFlushed(FLUSH0_NOT_YET_CONNECTED_EXCEPTION, true);
                    } else {
                        // Do not trigger channelWritabilityChanged because the channel is closed already.
                        outboundBuffer.failFlushed(FLUSH0_CLOSED_CHANNEL_EXCEPTION, false);
                    }
                } finally {
                    inFlush0 = false;
                }
                return;
            }

            //channel已经激活
            try {
                //开始写入数据
                doWrite(outboundBuffer);
            } catch (Throwable t) {
                if (t instanceof IOException && config().isAutoClose()) {
                    /**
                     * Just call {@link #close(ChannelPromise, Throwable, boolean)} here which will take care of
                     * failing all flushed messages and also ensure the actual close of the underlying transport
                     * will happen before the promises are notified.
                     *
                     * This is needed as otherwise {@link #isActive()} , {@link #isOpen()} and {@link #isWritable()}
                     * may still return {@code true} even if the channel should be closed as result of the exception.
                     */
                    close(voidPromise(), t, FLUSH0_CLOSED_CHANNEL_EXCEPTION, false);
                } else {
                    outboundBuffer.failFlushed(t, true);
                }
            } finally {
                inFlush0 = false;
            }
        }

        @Override
        public final ChannelPromise voidPromise() {
            assertEventLoop();

            return unsafeVoidPromise;
        }

        @Deprecated
        protected final boolean ensureOpen(ChannelPromise promise) {
            if (isOpen()) {
                return true;
            }

            safeSetFailure(promise, ENSURE_OPEN_CLOSED_CHANNEL_EXCEPTION);
            return false;
        }

        /**
         * Marks the specified {@code promise} as success.  If the {@code promise} is done already, log a message.
         */
        protected final void safeSetSuccess(ChannelPromise promise) {
            if (!(promise instanceof VoidChannelPromise) && !promise.trySuccess()) {
                logger.warn("Failed to mark a promise as success because it is done already: {}", promise);
            }
        }

        /**
         * Marks the specified {@code promise} as failure.  If the {@code promise} is done already, log a message.
         */
        protected final void safeSetFailure(ChannelPromise promise, Throwable cause) {
            if (!(promise instanceof VoidChannelPromise) && !promise.tryFailure(cause)) {
                logger.warn("Failed to mark a promise as failure because it's done already: {}", promise, cause);
            }
        }

        protected final void closeIfClosed() {
            if (isOpen()) {
                return;
            }
            close(voidPromise());
        }

        private void invokeLater(Runnable task) {
            try {
                // This method is used by outbound operation implementations to trigger an inbound event later.
                // They do not trigger an inbound event immediately because an outbound operation might have been
                // triggered by another inbound event handler method.  If fired immediately, the call stack
                // will look like this for example:
                //
                //   handlerA.inboundBufferUpdated() - (1) an inbound handler method closes a connection.
                //   -> handlerA.ctx.close()
                //      -> channel.unsafe.close()
                //         -> handlerA.channelInactive() - (2) another inbound handler method called while in (1) yet
                //
                // which means the execution of two inbound handler methods of the same handler overlap undesirably.
                eventLoop().execute(task);
            } catch (RejectedExecutionException e) {
                logger.warn("Can't invoke task later as EventLoop rejected it", e);
            }
        }

        /**
         * Appends the remote address to the message of the exceptions caused by connection attempt failure.
         */
        protected final Throwable annotateConnectException(Throwable cause, SocketAddress remoteAddress) {
            if (cause instanceof ConnectException) {
                return new AnnotatedConnectException((ConnectException) cause, remoteAddress);
            }
            if (cause instanceof NoRouteToHostException) {
                return new AnnotatedNoRouteToHostException((NoRouteToHostException) cause, remoteAddress);
            }
            if (cause instanceof SocketException) {
                return new AnnotatedSocketException((SocketException) cause, remoteAddress);
            }

            return cause;
        }

        /**
         * Prepares to close the {@link Channel}. If this method returns an {@link Executor}, the
         * caller must call the {@link Executor#execute(Runnable)} method with a task that calls
         * {@link #doClose()} on the returned {@link Executor}. If this method returns {@code null},
         * {@link #doClose()} must be called from the caller thread. (i.e. {@link EventLoop})
         */
        protected Executor prepareToClose() {
            return null;
        }
    }

    /**
     * Return {@code true} if the given {@link EventLoop} is compatible with this instance.
     */
    protected abstract boolean isCompatible(EventLoop loop);

    /**
     * Returns the {@link SocketAddress} which is bound locally.
     */
    protected abstract SocketAddress localAddress0();

    /**
     * Return the {@link SocketAddress} which the {@link Channel} is connected to.
     */
    protected abstract SocketAddress remoteAddress0();

    /**
     * Is called after the {@link Channel} is registered with its {@link EventLoop} as part of the register process.
     *
     * Sub-classes may override this method
     */
    protected void doRegister() throws Exception {
        // NOOP
    }

    /**
     * Bind the {@link Channel} to the {@link SocketAddress}
     */
    protected abstract void doBind(SocketAddress localAddress) throws Exception;

    /**
     * Disconnect this {@link Channel} from its remote peer
     */
    protected abstract void doDisconnect() throws Exception;

    /**
     * Close the {@link Channel}
     */
    protected abstract void doClose() throws Exception;

    /**
     * Deregister the {@link Channel} from its {@link EventLoop}.
     *
     * Sub-classes may override this method
     */
    protected void doDeregister() throws Exception {
        // NOOP
    }

    /**
     * Schedule a read operation.
     */
    protected abstract void doBeginRead() throws Exception;

    /**
     * Flush the content of the given buffer to the remote peer.
     */
    protected abstract void doWrite(ChannelOutboundBuffer in) throws Exception;

    /**
     * Invoked when a new message is added to a {@link ChannelOutboundBuffer} of this {@link AbstractChannel}, so that
     * the {@link Channel} implementation converts the message to another. (e.g. heap buffer -> direct buffer)
     */
    protected Object filterOutboundMessage(Object msg) throws Exception {
        return msg;
    }

    static final class CloseFuture extends DefaultChannelPromise {

        CloseFuture(AbstractChannel ch) {
            super(ch);
        }

        @Override
        public ChannelPromise setSuccess() {
            throw new IllegalStateException();
        }

        @Override
        public ChannelPromise setFailure(Throwable cause) {
            throw new IllegalStateException();
        }

        @Override
        public boolean trySuccess() {
            throw new IllegalStateException();
        }

        @Override
        public boolean tryFailure(Throwable cause) {
            throw new IllegalStateException();
        }

        boolean setClosed() {
            return super.trySuccess();
        }
    }

    private static final class AnnotatedConnectException extends ConnectException {

        private static final long serialVersionUID = 3901958112696433556L;

        AnnotatedConnectException(ConnectException exception, SocketAddress remoteAddress) {
            super(exception.getMessage() + ": " + remoteAddress);
            setStackTrace(exception.getStackTrace());
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class AnnotatedNoRouteToHostException extends NoRouteToHostException {

        private static final long serialVersionUID = -6801433937592080623L;

        AnnotatedNoRouteToHostException(NoRouteToHostException exception, SocketAddress remoteAddress) {
            super(exception.getMessage() + ": " + remoteAddress);
            setStackTrace(exception.getStackTrace());
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class AnnotatedSocketException extends SocketException {

        private static final long serialVersionUID = 3896743275010454039L;

        AnnotatedSocketException(SocketException exception, SocketAddress remoteAddress) {
            super(exception.getMessage() + ": " + remoteAddress);
            setStackTrace(exception.getStackTrace());
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }
}
