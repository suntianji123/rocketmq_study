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
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.OrderedEventExecutor;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.PromiseNotificationUtil;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.channel.DefaultChannelPipeline.*;

abstract class AbstractChannelHandlerContext extends DefaultAttributeMap implements ChannelHandlerContext {

    volatile AbstractChannelHandlerContext next;
    volatile AbstractChannelHandlerContext prev;

    /**
     * channelhandlercontext的状态 初始化为init 当handler添加到channel的pipeline后状态为ADDED
     */
    private static final AtomicIntegerFieldUpdater<AbstractChannelHandlerContext> HANDLER_STATE_UPDATER;

    static {
        AtomicIntegerFieldUpdater<AbstractChannelHandlerContext> handlerStateUpdater = PlatformDependent
                .newAtomicIntegerFieldUpdater(AbstractChannelHandlerContext.class, "handlerState");
        if (handlerStateUpdater == null) {
            handlerStateUpdater = AtomicIntegerFieldUpdater
                    .newUpdater(AbstractChannelHandlerContext.class, "handlerState");
        }
        HANDLER_STATE_UPDATER = handlerStateUpdater;
    }

    /**
     * {@link ChannelHandler#handlerAdded(ChannelHandlerContext)} is about to be called.
     */
    private static final int ADD_PENDING = 1;
    /**
     * {@link ChannelHandler#handlerAdded(ChannelHandlerContext)} was called.
     */
    private static final int ADD_COMPLETE = 2;
    /**
     * {@link ChannelHandler#handlerRemoved(ChannelHandlerContext)} was called.
     */
    private static final int REMOVE_COMPLETE = 3;
    /**
     * Neither {@link ChannelHandler#handlerAdded(ChannelHandlerContext)}
     * nor {@link ChannelHandler#handlerRemoved(ChannelHandlerContext)} was called.
     */
    private static final int INIT = 0;

    /**
     * 是否为输入的channelhandlercontext
     */
    private final boolean inbound;

    /**
     * 是否为输出的channelhandlerContext
     */
    private final boolean outbound;

    /**
     * channelhandlercontext所位于的channelpipeline的channelhandlercontext链表
     */
    private final DefaultChannelPipeline pipeline;

    /**
     * 名字
     */
    private final String name;

    /**
     * 是否有序
     */
    private final boolean ordered;

    /**
     * 执行handler方法的执行器
     */
    final EventExecutor executor;
    private ChannelFuture succeededFuture;

    // Lazily instantiated tasks used to trigger events to a handler with different executor.
    // There is no need to make this volatile as at worse it will just create a few more instances then needed.
    private Runnable invokeChannelReadCompleteTask;
    private Runnable invokeReadTask;

    /**
     * 执行channel的输出缓冲区可写状态变更的任务体
     */
    private Runnable invokeChannelWritableStateChangedTask;
    private Runnable invokeFlushTask;

    private volatile int handlerState = INIT;

    /**
     * 实例化一个默认的channdlerHandlerContext对象
     * @param pipeline channelhandlercontext所位于的channelpipeline的channelhandlerContext链表
     * @param executor 执行器handler方法的执行器
     * @param name 名字
     * @param inbound 是否为输入handler
     * @param outbound 是否输出handler
     */
    AbstractChannelHandlerContext(DefaultChannelPipeline pipeline, EventExecutor executor, String name,
                                  boolean inbound, boolean outbound) {
        //设置名字
        this.name = ObjectUtil.checkNotNull(name, "name");
        //设置pipeline
        this.pipeline = pipeline;
        //设置执行器
        this.executor = executor;
        //设置是否为输入
        this.inbound = inbound;
        //设置是否为输出
        this.outbound = outbound;
        // Its ordered if its driven by the EventLoop or the given Executor is an instanceof OrderedEventExecutor.
        //设置执行方法是否是有序的
        ordered = executor == null || executor instanceof OrderedEventExecutor;
    }

    @Override
    public Channel channel() {
        return pipeline.channel();
    }

    @Override
    public ChannelPipeline pipeline() {
        return pipeline;
    }

    @Override
    public ByteBufAllocator alloc() {
        return channel().config().getAllocator();
    }

    @Override
    public EventExecutor executor() {
        if (executor == null) {
            return channel().eventLoop();
        } else {
            return executor;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ChannelHandlerContext fireChannelRegistered() {
        invokeChannelRegistered(findContextInbound());
        return this;
    }

    static void invokeChannelRegistered(final AbstractChannelHandlerContext next) {
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelRegistered();
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeChannelRegistered();
                }
            });
        }
    }

    private void invokeChannelRegistered() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelRegistered(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelRegistered();
        }
    }

    @Override
    public ChannelHandlerContext fireChannelUnregistered() {
        invokeChannelUnregistered(findContextInbound());
        return this;
    }

    static void invokeChannelUnregistered(final AbstractChannelHandlerContext next) {
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelUnregistered();
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeChannelUnregistered();
                }
            });
        }
    }

    private void invokeChannelUnregistered() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelUnregistered(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelUnregistered();
        }
    }

    @Override
    public ChannelHandlerContext fireChannelActive() {
        final AbstractChannelHandlerContext next = findContextInbound();
        invokeChannelActive(next);
        return this;
    }

    static void invokeChannelActive(final AbstractChannelHandlerContext next) {
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelActive();
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeChannelActive();
                }
            });
        }
    }

    private void invokeChannelActive() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelActive(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelActive();
        }
    }

    @Override
    public ChannelHandlerContext fireChannelInactive() {
        invokeChannelInactive(findContextInbound());
        return this;
    }

    static void invokeChannelInactive(final AbstractChannelHandlerContext next) {
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelInactive();
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeChannelInactive();
                }
            });
        }
    }

    private void invokeChannelInactive() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelInactive(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelInactive();
        }
    }

    @Override
    public ChannelHandlerContext fireExceptionCaught(final Throwable cause) {
        invokeExceptionCaught(next, cause);
        return this;
    }

    static void invokeExceptionCaught(final AbstractChannelHandlerContext next, final Throwable cause) {
        ObjectUtil.checkNotNull(cause, "cause");
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeExceptionCaught(cause);
        } else {
            try {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        next.invokeExceptionCaught(cause);
                    }
                });
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to submit an exceptionCaught() event.", t);
                    logger.warn("The exceptionCaught() event that was failed to submit was:", cause);
                }
            }
        }
    }

    private void invokeExceptionCaught(final Throwable cause) {
        if (invokeHandler()) {
            try {
                handler().exceptionCaught(this, cause);
            } catch (Throwable error) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "An exception {}" +
                        "was thrown by a user handler's exceptionCaught() " +
                        "method while handling the following exception:",
                        ThrowableUtil.stackTraceToString(error), cause);
                } else if (logger.isWarnEnabled()) {
                    logger.warn(
                        "An exception '{}' [enable DEBUG level for full stacktrace] " +
                        "was thrown by a user handler's exceptionCaught() " +
                        "method while handling the following exception:", error, cause);
                }
            }
        } else {
            fireExceptionCaught(cause);
        }
    }

    @Override
    public ChannelHandlerContext fireUserEventTriggered(final Object event) {
        invokeUserEventTriggered(findContextInbound(), event);
        return this;
    }

    static void invokeUserEventTriggered(final AbstractChannelHandlerContext next, final Object event) {
        ObjectUtil.checkNotNull(event, "event");
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeUserEventTriggered(event);
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeUserEventTriggered(event);
                }
            });
        }
    }

    private void invokeUserEventTriggered(Object event) {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).userEventTriggered(this, event);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireUserEventTriggered(event);
        }
    }

    @Override
    public ChannelHandlerContext fireChannelRead(final Object msg) {
        invokeChannelRead(findContextInbound(), msg);
        return this;
    }
    static void invokeChannelRead(final AbstractChannelHandlerContext next, final Object msg) {
        ObjectUtil.checkNotNull(msg, "msg");
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelRead(msg);
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    next.invokeChannelRead(msg);
                }
            });
        }
    }

    private void invokeChannelRead(Object msg) {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelRead(this, msg);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelRead(msg);
        }
    }

    @Override
    public ChannelHandlerContext fireChannelReadComplete() {
        invokeChannelReadComplete(findContextInbound());
        return this;
    }

    static void invokeChannelReadComplete(final AbstractChannelHandlerContext next) {
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelReadComplete();
        } else {
            Runnable task = next.invokeChannelReadCompleteTask;
            if (task == null) {
                next.invokeChannelReadCompleteTask = task = new Runnable() {
                    @Override
                    public void run() {
                        next.invokeChannelReadComplete();
                    }
                };
            }
            executor.execute(task);
        }
    }

    private void invokeChannelReadComplete() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelReadComplete(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelReadComplete();
        }
    }

    @Override
    public ChannelHandlerContext fireChannelWritabilityChanged() {
        invokeChannelWritabilityChanged(findContextInbound());
        return this;
    }

    /**
     * 下发channel的输出缓冲区可写状态变更的消息
     * @param next channelhandlerContext对象
     */
    static void invokeChannelWritabilityChanged(final AbstractChannelHandlerContext next) {
        //获取channelhandlerContext的执行器
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeChannelWritabilityChanged();
        } else {
            Runnable task = next.invokeChannelWritableStateChangedTask;
            if (task == null) {
                next.invokeChannelWritableStateChangedTask = task = new Runnable() {
                    @Override
                    public void run() {
                        next.invokeChannelWritabilityChanged();
                    }
                };
            }
            executor.execute(task);
        }
    }

    /**
     * 执行channel的输出缓冲区可写状态变更的处理
     */
    private void invokeChannelWritabilityChanged() {
        if (invokeHandler()) {
            try {
                ((ChannelInboundHandler) handler()).channelWritabilityChanged(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            fireChannelWritabilityChanged();
        }
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return bind(localAddress, newPromise());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return connect(remoteAddress, newPromise());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return connect(remoteAddress, localAddress, newPromise());
    }

    @Override
    public ChannelFuture disconnect() {
        return disconnect(newPromise());
    }

    @Override
    public ChannelFuture close() {
        return close(newPromise());
    }

    @Override
    public ChannelFuture deregister() {
        return deregister(newPromise());
    }

    /**
     * 绑定端口
     * @param localAddress 端口地址
     * @param promise 绑定的异步操作对象
     * @return
     */
    @Override
    public ChannelFuture bind(final SocketAddress localAddress, final ChannelPromise promise) {
        if (localAddress == null) {//本地服务器地址为null
            throw new NullPointerException("localAddress");
        }

        //验证promise的合理性
        if (!validatePromise(promise, false)) {
            // 取消操作
            return promise;
        }

        //查找channelhandlerContext下一个输出的channelhandlercontext对象
        final AbstractChannelHandlerContext next = findContextOutbound();

        //获取执行器对象
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeBind(localAddress, promise);
        } else {
            safeExecute(executor, new Runnable() {
                @Override
                public void run() {
                    next.invokeBind(localAddress, promise);
                }
            }, promise, null);
        }
        return promise;
    }

    /**
     * 执行绑定方法
     * @param localAddress 端口地址
     * @param promise 绑定端口的异步操作对象
     */
    private void invokeBind(SocketAddress localAddress, ChannelPromise promise) {
        if (invokeHandler()) {//channel必须可用
            try {
                //调用channelhandlercontext的handler的bind方法
                ((ChannelOutboundHandler) handler()).bind(this, localAddress, promise);
            } catch (Throwable t) {
                notifyOutboundHandlerException(t, promise);
            }
        } else {
            bind(localAddress, promise);
        }
    }

    /**
     * 连接远程服务器
     * @param remoteAddress 远程服务器地址
     * @param promise 连接的异步操作对象
     * @return
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return connect(remoteAddress, null, promise);
    }

    /**
     * 连接远程服务器
     * @param remoteAddress 远程服务器地址
     * @param localAddress 本地地址
     * @param promise 连接的异步操作对象
     * @return
     */
    @Override
    public ChannelFuture connect(
            final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) {

        if (remoteAddress == null) {//远程服务器地址不能为空
            throw new NullPointerException("remoteAddress");
        }
        //判断连接的异步操作对象是否合理
        if (!validatePromise(promise, false)) {
            // cancelled
            return promise;
        }

        //寻找下一个输出的handler
        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeConnect(remoteAddress, localAddress, promise);
        } else {
            safeExecute(executor, new Runnable() {
                @Override
                public void run() {
                    next.invokeConnect(remoteAddress, localAddress, promise);
                }
            }, promise, null);
        }
        return promise;
    }

    /**
     * 执行连接远程服务器 调用handler的connect方法
     * @param remoteAddress 远程服务器地址
     * @param localAddress 本地地址
     * @param promise 连接的异步操作对象
     */
    private void invokeConnect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        if (invokeHandler()) {
            try {
                ((ChannelOutboundHandler) handler()).connect(this, remoteAddress, localAddress, promise);
            } catch (Throwable t) {
                notifyOutboundHandlerException(t, promise);
            }
        } else {
            connect(remoteAddress, localAddress, promise);
        }
    }

    @Override
    public ChannelFuture disconnect(final ChannelPromise promise) {
        if (!validatePromise(promise, false)) {
            // cancelled
            return promise;
        }

        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            // Translate disconnect to close if the channel has no notion of disconnect-reconnect.
            // So far, UDP/IP is the only transport that has such behavior.
            if (!channel().metadata().hasDisconnect()) {
                next.invokeClose(promise);
            } else {
                next.invokeDisconnect(promise);
            }
        } else {
            safeExecute(executor, new Runnable() {
                @Override
                public void run() {
                    if (!channel().metadata().hasDisconnect()) {
                        next.invokeClose(promise);
                    } else {
                        next.invokeDisconnect(promise);
                    }
                }
            }, promise, null);
        }
        return promise;
    }

    private void invokeDisconnect(ChannelPromise promise) {
        if (invokeHandler()) {
            try {
                ((ChannelOutboundHandler) handler()).disconnect(this, promise);
            } catch (Throwable t) {
                notifyOutboundHandlerException(t, promise);
            }
        } else {
            disconnect(promise);
        }
    }

    @Override
    public ChannelFuture close(final ChannelPromise promise) {
        if (!validatePromise(promise, false)) {
            // cancelled
            return promise;
        }

        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeClose(promise);
        } else {
            safeExecute(executor, new Runnable() {
                @Override
                public void run() {
                    next.invokeClose(promise);
                }
            }, promise, null);
        }

        return promise;
    }

    private void invokeClose(ChannelPromise promise) {
        if (invokeHandler()) {
            try {
                ((ChannelOutboundHandler) handler()).close(this, promise);
            } catch (Throwable t) {
                notifyOutboundHandlerException(t, promise);
            }
        } else {
            close(promise);
        }
    }

    @Override
    public ChannelFuture deregister(final ChannelPromise promise) {
        if (!validatePromise(promise, false)) {
            // cancelled
            return promise;
        }

        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeDeregister(promise);
        } else {
            safeExecute(executor, new Runnable() {
                @Override
                public void run() {
                    next.invokeDeregister(promise);
                }
            }, promise, null);
        }

        return promise;
    }

    private void invokeDeregister(ChannelPromise promise) {
        if (invokeHandler()) {
            try {
                ((ChannelOutboundHandler) handler()).deregister(this, promise);
            } catch (Throwable t) {
                notifyOutboundHandlerException(t, promise);
            }
        } else {
            deregister(promise);
        }
    }

    @Override
    public ChannelHandlerContext read() {
        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeRead();
        } else {
            Runnable task = next.invokeReadTask;
            if (task == null) {
                next.invokeReadTask = task = new Runnable() {
                    @Override
                    public void run() {
                        next.invokeRead();
                    }
                };
            }
            executor.execute(task);
        }

        return this;
    }

    private void invokeRead() {
        if (invokeHandler()) {
            try {
                ((ChannelOutboundHandler) handler()).read(this);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            read();
        }
    }

    @Override
    public ChannelFuture write(Object msg) {
        return write(msg, newPromise());
    }

    @Override
    public ChannelFuture write(final Object msg, final ChannelPromise promise) {
        if (msg == null) {
            throw new NullPointerException("msg");
        }

        try {
            if (!validatePromise(promise, true)) {
                ReferenceCountUtil.release(msg);
                // cancelled
                return promise;
            }
        } catch (RuntimeException e) {
            ReferenceCountUtil.release(msg);
            throw e;
        }
        write(msg, false, promise);

        return promise;
    }

    /**
     * 执行向channel中写入消息对象的方法
     * @param msg 写入的数据对象
     * @param promise 写入的异步操作对象
     */
    private void invokeWrite(Object msg, ChannelPromise promise) {
        if (invokeHandler()) {
            //执行向channel中写入数据
            invokeWrite0(msg, promise);
        } else {
            write(msg, promise);
        }
    }

    /**
     * 执行向channel中写入数据
     * @param msg 需要向channel中写入的数据对象
     * @param promise 向channel写入数据的异步操作对象
     */
    private void invokeWrite0(Object msg, ChannelPromise promise) {
        try {
            //获取handler 执行handler的write方法
            ((ChannelOutboundHandler) handler()).write(this, msg, promise);
        } catch (Throwable t) {
            notifyOutboundHandlerException(t, promise);
        }
    }

    @Override
    public ChannelHandlerContext flush() {
        final AbstractChannelHandlerContext next = findContextOutbound();
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            next.invokeFlush();
        } else {
            Runnable task = next.invokeFlushTask;
            if (task == null) {
                next.invokeFlushTask = task = new Runnable() {
                    @Override
                    public void run() {
                        next.invokeFlush();
                    }
                };
            }
            safeExecute(executor, task, channel().voidPromise(), null);
        }

        return this;
    }

    /**
     * 执行刷新channel的输出缓冲区
     */
    private void invokeFlush() {
        if (invokeHandler()) {
            invokeFlush0();
        } else {
            flush();
        }
    }

    /**
     * 执行刷新channel的缓冲区
     */
    private void invokeFlush0() {
        try {
            //找到context的handler对象  执行它的flush方法
            ((ChannelOutboundHandler) handler()).flush(this);
        } catch (Throwable t) {
            notifyHandlerException(t);
        }
    }

    /**
     * 向channel中写入数据
     * @param msg 数据对象
     * @param promise 写入数据异步操作对象
     * @return
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        if (msg == null) {//数据对象不能为空
            throw new NullPointerException("msg");
        }

        //验证异步操作对象的合理性
        if (!validatePromise(promise, true)) {
            //释放消息对象
            ReferenceCountUtil.release(msg);
            // 取消异步操作
            return promise;
        }

        //向channel中写入数据
        write(msg, true, promise);

        //返回写入数据的异步操作对象
        return promise;
    }

    private void invokeWriteAndFlush(Object msg, ChannelPromise promise) {
        if (invokeHandler()) {
            invokeWrite0(msg, promise);
            invokeFlush0();
        } else {
            writeAndFlush(msg, promise);
        }
    }

    /**
     * 向channel中写入数据
     * @param msg 数据对象
     * @param flush 是否强制刷新缓冲区
     * @param promise 向channel写入数据的异步操作对象
     */
    private void write(Object msg, boolean flush, ChannelPromise promise) {
        //获取下一个输出的channelhandlerContext
        AbstractChannelHandlerContext next = findContextOutbound();
        //获取执行器
        EventExecutor executor = next.executor();
        if (executor.inEventLoop()) {
            if (flush) {
                next.invokeWriteAndFlush(msg, promise);
            } else {
                next.invokeWrite(msg, promise);
            }
        } else {
            //实例化一个task对象
            AbstractWriteTask task;
            if (flush) {//强制刷新输出缓冲区
                task = WriteAndFlushTask.newInstance(next, msg, promise);
            }  else {
                task = WriteTask.newInstance(next, msg, promise);
            }
            //安全的执行任务
            safeExecute(executor, task, promise, msg);
        }
    }

    /**
     * 向channel中写入数据
     * @param msg 数据对象
     * @return
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return writeAndFlush(msg, newPromise());
    }

    private static void notifyOutboundHandlerException(Throwable cause, ChannelPromise promise) {
        if (!(promise instanceof VoidChannelPromise)) {
            PromiseNotificationUtil.tryFailure(promise, cause, logger);
        }
    }

    private void notifyHandlerException(Throwable cause) {
        if (inExceptionCaught(cause)) {
            if (logger.isWarnEnabled()) {
                logger.warn(
                        "An exception was thrown by a user handler " +
                                "while handling an exceptionCaught event", cause);
            }
            return;
        }

        invokeExceptionCaught(cause);
    }

    private static boolean inExceptionCaught(Throwable cause) {
        do {
            StackTraceElement[] trace = cause.getStackTrace();
            if (trace != null) {
                for (StackTraceElement t : trace) {
                    if (t == null) {
                        break;
                    }
                    if ("exceptionCaught".equals(t.getMethodName())) {
                        return true;
                    }
                }
            }

            cause = cause.getCause();
        } while (cause != null);

        return false;
    }

    @Override
    public ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel(), executor());
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return new DefaultChannelProgressivePromise(channel(), executor());
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        ChannelFuture succeededFuture = this.succeededFuture;
        if (succeededFuture == null) {
            this.succeededFuture = succeededFuture = new SucceededChannelFuture(channel(), executor());
        }
        return succeededFuture;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return new FailedChannelFuture(channel(), executor(), cause);
    }

    /**
     * 验证异步操作对象
     * @param promise 被验证的异步操作对象
     * @param allowVoidPromise 是否允许voidPromise
     * @return
     */
    private boolean validatePromise(ChannelPromise promise, boolean allowVoidPromise) {
        if (promise == null) {//异步操作对象不能为空
            throw new NullPointerException("promise");
        }

        if (promise.isDone()) {//如果异步操作对象已经执行完成
            // Check if the promise was cancelled and if so signal that the processing of the operation
            // should not be performed.
            //
            // See https://github.com/netty/netty/issues/2349
            if (promise.isCancelled()) {//如果异步操作对象被取消 返回false
                return false;
            }

            //抛出异常
            throw new IllegalArgumentException("promise already done: " + promise);
        }

        //异步操作对象的channel必须为channelhandlerContext的channel
        if (promise.channel() != channel()) {
            throw new IllegalArgumentException(String.format(
                    "promise.channel does not match: %s (expected: %s)", promise.channel(), channel()));
        }

        //如果是DefaultChannelPromise返回true
        if (promise.getClass() == DefaultChannelPromise.class) {
            return true;
        }

        //如果不允许voidPromise但是传入的异步操作对象的VoidPromise 抛出异常
        if (!allowVoidPromise && promise instanceof VoidChannelPromise) {
            throw new IllegalArgumentException(
                    StringUtil.simpleClassName(VoidChannelPromise.class) + " not allowed for this operation");
        }

        //如果异步操作对象是关闭的异步操作 抛出异常
        if (promise instanceof AbstractChannel.CloseFuture) {
            throw new IllegalArgumentException(
                    StringUtil.simpleClassName(AbstractChannel.CloseFuture.class) + " not allowed in a pipeline");
        }
        return true;
    }

    private AbstractChannelHandlerContext findContextInbound() {
        AbstractChannelHandlerContext ctx = this;
        do {
            ctx = ctx.next;
        } while (!ctx.inbound);
        return ctx;
    }

    private AbstractChannelHandlerContext findContextOutbound() {
        AbstractChannelHandlerContext ctx = this;
        do {
            ctx = ctx.prev;
        } while (!ctx.outbound);
        return ctx;
    }

    @Override
    public ChannelPromise voidPromise() {
        return channel().voidPromise();
    }

    final void setRemoved() {
        handlerState = REMOVE_COMPLETE;
    }

    final void setAddComplete() {
        for (;;) {
            int oldState = handlerState;
            // Ensure we never update when the handlerState is REMOVE_COMPLETE already.
            // oldState is usually ADD_PENDING but can also be REMOVE_COMPLETE when an EventExecutor is used that is not
            // exposing ordering guarantees.
            if (oldState == REMOVE_COMPLETE || HANDLER_STATE_UPDATER.compareAndSet(this, oldState, ADD_COMPLETE)) {
                return;
            }
        }
    }

    final void setAddPending() {
        boolean updated = HANDLER_STATE_UPDATER.compareAndSet(this, INIT, ADD_PENDING);
        assert updated; // This should always be true as it MUST be called before setAddComplete() or setRemoved().
    }

    /**
     * 判断handler是否为可执行的handler
     * @return
     */
    private boolean invokeHandler() {
        // Store in local variable to reduce volatile reads.
        int handlerState = this.handlerState;
        return handlerState == ADD_COMPLETE || (!ordered && handlerState == ADD_PENDING);
    }

    @Override
    public boolean isRemoved() {
        return handlerState == REMOVE_COMPLETE;
    }

    private static void safeExecute(EventExecutor executor, Runnable runnable, ChannelPromise promise, Object msg) {
        try {
            executor.execute(runnable);
        } catch (Throwable cause) {
            try {
                promise.setFailure(cause);
            } finally {
                if (msg != null) {
                    ReferenceCountUtil.release(msg);
                }
            }
        }
    }

    /**
     * 向channel写入数据的任务类
     */
    abstract static class AbstractWriteTask implements Runnable {

        /**
         * 当向channel的输出ByteBuf写入数据的时候 估算提交数据的大小
         */
        private static final boolean ESTIMATE_TASK_SIZE_ON_SUBMIT =
                SystemPropertyUtil.getBoolean("io.netty.transport.estimateSizeOnSubmit", true);

        //假设有一个64位JVM，16个字节的对象标头，3个参考字段和一个int字段，再加上对齐方式 累计48个字节
        // Assuming a 64-bit JVM, 16 bytes object header, 3 reference fields and one int field, plus alignment
        private static final int WRITE_TASK_OVERHEAD =
                SystemPropertyUtil.getInt("io.netty.transport.writeTaskSizeOverhead", 48);

        /**
         * 处理回收
         */
        private final Recycler.Handle handle;

        /**
         * 任务所属的ChannelHandlerContext对象
         */
        private AbstractChannelHandlerContext ctx;

        /**
         * 需要写入到缓冲区的数据对象
         */
        private Object msg;

        /**
         * 写入数据的异步操作对象
         */
        private ChannelPromise promise;

        /**
         * 写入到chanel的msg对象的大小
         */
        private int size;

        private AbstractWriteTask(Recycler.Handle handle) {
            this.handle = handle;
        }

        /**
         * 从回收站获取一个任务后 对任务的初始化的方法
         * @param task 被初始化的任务对象
         * @param ctx 被写入数据的channel的channelhandlercontext对象
         * @param msg 数据对象
         * @param promise 写入的异步操作对象
         */
        protected static void init(AbstractWriteTask task, AbstractChannelHandlerContext ctx,
                                   Object msg, ChannelPromise promise) {
            //设置任务的channelhandlercontext读写
            task.ctx = ctx;
            //设置被写入的数据对象
            task.msg = msg;
            //设置写入的异步操作对象
            task.promise = promise;

            //在提交任务时候 需要估算任务的大小
            if (ESTIMATE_TASK_SIZE_ON_SUBMIT) {
                //获取channel的出站缓冲区对象
                ChannelOutboundBuffer buffer = ctx.channel().unsafe().outboundBuffer();

                // Check for null as it may be set to null if the channel is closed already
                if (buffer != null) {//出站缓冲区不为null
                    task.size = ctx.pipeline.estimatorHandle().size(msg) + WRITE_TASK_OVERHEAD;
                    buffer.incrementPendingOutboundBytes(task.size);
                } else {//出站缓冲区为null  任务的数据大小为0
                    task.size = 0;
                }
            } else {
                task.size = 0;
            }
        }

        @Override
        public final void run() {
            try {
                //获取channel的输出缓冲区
                ChannelOutboundBuffer buffer = ctx.channel().unsafe().outboundBuffer();

                //判断向输出缓冲区的写入的字节是否超过channel配置的高水位值 64M 如果超过了 下发channel可写状态变更的事件
                if (ESTIMATE_TASK_SIZE_ON_SUBMIT && buffer != null) {
                    buffer.decrementPendingOutboundBytes(size);
                }

                //向channel的输出缓冲区写入数据
                write(ctx, msg, promise);
            } finally {
                // Set to null so the GC can collect them directly
                ctx = null;
                msg = null;
                promise = null;
                recycle(handle);
            }
        }

        /**
         * 向channel的输出缓冲区写入数据的方法体
         * @param ctx 当前channelhandlerContext对象
         * @param msg 写入到channel的数据对象
         * @param promise 异步操作对象
         */
        protected void write(AbstractChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            //执行写入消息
            ctx.invokeWrite(msg, promise);
        }

        protected abstract void recycle(Recycler.Handle handle);
    }

    static final class WriteTask extends AbstractWriteTask implements SingleThreadEventLoop.NonWakeupRunnable {

        private static final Recycler<WriteTask> RECYCLER = new Recycler<WriteTask>() {
            @Override
            protected WriteTask newObject(Handle handle) {
                return new WriteTask(handle);
            }
        };

        private static WriteTask newInstance(
                AbstractChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            WriteTask task = RECYCLER.get();
            init(task, ctx, msg, promise);
            return task;
        }

        private WriteTask(Recycler.Handle handle) {
            super(handle);
        }

        @Override
        protected void recycle(Recycler.Handle handle) {
            RECYCLER.recycle(this, handle);
        }
    }

    static final class WriteAndFlushTask extends AbstractWriteTask {

        private static final Recycler<WriteAndFlushTask> RECYCLER = new Recycler<WriteAndFlushTask>() {
            @Override
            protected WriteAndFlushTask newObject(Handle handle) {
                return new WriteAndFlushTask(handle);
            }
        };

        /**
         * 向channel写入数据并且刷新缓冲区的任务
         * @param ctx channelhandlerContext对象
         * @param msg 数据对象
         * @param promise 写入数据的异步操作对象
         * @return
         */
        private static WriteAndFlushTask newInstance(
                AbstractChannelHandlerContext ctx, Object msg,  ChannelPromise promise) {
            //从回收站获取一个写入channel并且刷新缓冲区的任务
            WriteAndFlushTask task = RECYCLER.get();
            init(task, ctx, msg, promise);
            return task;
        }

        private WriteAndFlushTask(Recycler.Handle handle) {
            super(handle);
        }

        @Override
        public void write(AbstractChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            super.write(ctx, msg, promise);

            //执行刷新缓冲区
            ctx.invokeFlush();
        }

        @Override
        protected void recycle(Recycler.Handle handle) {
            RECYCLER.recycle(this, handle);
        }
    }
}
