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
package io.netty.handler.timeout;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Triggers an {@link IdleStateEvent} when a {@link Channel} has not performed
 * read, write, or both operation for a while.
 *
 * <h3>Supported idle states</h3>
 * <table border="1">
 * <tr>
 * <th>Property</th><th>Meaning</th>
 * </tr>
 * <tr>
 * <td>{@code readerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#READER_IDLE}
 *     will be triggered when no read was performed for the specified period of
 *     time.  Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code writerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#WRITER_IDLE}
 *     will be triggered when no write was performed for the specified period of
 *     time.  Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code allIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE}
 *     will be triggered when neither read nor write was performed for the
 *     specified period of time.  Specify {@code 0} to disable.</td>
 * </tr>
 * </table>
 *
 * <pre>
 * // An example that sends a ping message when there is no outbound traffic
 * // for 30 seconds.  The connection is closed when there is no inbound traffic
 * // for 60 seconds.
 *
 * public class MyChannelInitializer extends {@link ChannelInitializer}&lt;{@link Channel}&gt; {
 *     {@code @Override}
 *     public void initChannel({@link Channel} channel) {
 *         channel.pipeline().addLast("idleStateHandler", new {@link IdleStateHandler}(60, 30, 0));
 *         channel.pipeline().addLast("myHandler", new MyHandler());
 *     }
 * }
 *
 * // Handler should handle the {@link IdleStateEvent} triggered by {@link IdleStateHandler}.
 * public class MyHandler extends {@link ChannelDuplexHandler} {
 *     {@code @Override}
 *     public void userEventTriggered({@link ChannelHandlerContext} ctx, {@link Object} evt) throws {@link Exception} {
 *         if (evt instanceof {@link IdleStateEvent}) {
 *             {@link IdleStateEvent} e = ({@link IdleStateEvent}) evt;
 *             if (e.state() == {@link IdleState}.READER_IDLE) {
 *                 ctx.close();
 *             } else if (e.state() == {@link IdleState}.WRITER_IDLE) {
 *                 ctx.writeAndFlush(new PingMessage());
 *             }
 *         }
 *     }
 * }
 *
 * {@link ServerBootstrap} bootstrap = ...;
 * ...
 * bootstrap.childHandler(new MyChannelInitializer());
 * ...
 * </pre>
 *
 * @see ReadTimeoutHandler
 * @see WriteTimeoutHandler
 */
public class IdleStateHandler extends ChannelDuplexHandler {
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * 向channel的输出缓冲区写入数据后 执行的回调方法
     */
    private final ChannelFutureListener writeListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            //更新最后一次向channel写入数据的时间戳
            lastWriteTime = System.nanoTime();
            //设置idle标志 等待下一channel没有发生io时候 去更新
            firstWriterIdleEvent = firstAllIdleEvent = true;
        }
    };

    /**
     * 读空闲时间毫秒数
     */
    private final long readerIdleTimeNanos;

    /**
     * 写空闲时间毫秒数
     */
    private final long writerIdleTimeNanos;

    /**
     * 所有空闲时间毫秒数
     */
    private final long allIdleTimeNanos;

    private ScheduledFuture<?> readerIdleTimeout;
    /**
     * 最后一次从channel读取数据的时间戳
     */
    private long lastReadTime;

    /**
     * channel第一次出现在指定的事件段内没有发生任何io读时间
     */
    private boolean firstReaderIdleEvent = true;

    private ScheduledFuture<?> writerIdleTimeout;

    /**
     * 最后一次向channel写入数据的时间戳
     */
    private long lastWriteTime;

    /**
     * channel是否在指定超时时间内 没有发生任何写时间
     */
    private boolean firstWriterIdleEvent = true;

    /**
     * 所有空闲的定时任务异步操作结果对象
     */
    private ScheduledFuture<?> allIdleTimeout;

    /**
     * 是否为channel的第一个idle 事件 也就是说 在120秒第一次出现channel没有发生任何读写事件
     */
    private boolean firstAllIdleEvent = true;

    private byte state; // 0 - none, 1 - initialized, 2 - destroyed

    /**
     * handler是否正在读数据
     */
    private boolean reading;


    /**
     * 实例化一个handler
     * @param readerIdleTimeSeconds 读空闲时间秒数
     * @param writerIdleTimeSeconds 写空闲时间秒数
     * @param allIdleTimeSeconds 所有空闲时间秒数 默认为120
     */
    public IdleStateHandler(
            int readerIdleTimeSeconds,
            int writerIdleTimeSeconds,
            int allIdleTimeSeconds) {

        this(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds,
             TimeUnit.SECONDS);
    }


    /**
     * 实例化一个idle handler对象
     * @param readerIdleTime 读空闲时间
     * @param writerIdleTime 写空闲时间
     * @param allIdleTime 所有空闲时间
     * @param unit 时间单位
     */
    public IdleStateHandler(
            long readerIdleTime, long writerIdleTime, long allIdleTime,
            TimeUnit unit) {
        if (unit == null) {//时间单位不能为空
            throw new NullPointerException("unit");
        }
        if (readerIdleTime <= 0) {//读空闲时间小于0
            //读空闲时间毫秒数为0
            readerIdleTimeNanos = 0;
        } else {
            //读空闲时间毫秒数
            readerIdleTimeNanos = Math.max(unit.toNanos(readerIdleTime), MIN_TIMEOUT_NANOS);
        }
        if (writerIdleTime <= 0) {//写空闲时间秒数小于0
            //写空闲时间为0
            writerIdleTimeNanos = 0;
        } else {
            //写空闲时间秒数大于0
            writerIdleTimeNanos = Math.max(unit.toNanos(writerIdleTime), MIN_TIMEOUT_NANOS);
        }

        //所有空闲时间毫秒双联户
        if (allIdleTime <= 0) {
            allIdleTimeNanos = 0;
        } else {
            //所有空闲时间毫秒数
            allIdleTimeNanos = Math.max(unit.toNanos(allIdleTime), MIN_TIMEOUT_NANOS);
        }
    }

    /**
     * Return the readerIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getReaderIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(readerIdleTimeNanos);
    }

    /**
     * Return the writerIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getWriterIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(writerIdleTimeNanos);
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getAllIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(allIdleTimeNanos);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            // channelActvie() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            initialize(ctx);
        } else {
            // channelActive() event has not been fired yet.  this.channelActive() will be invoked
            // and initialization will occur there.
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            //如果在2分钟之内 channel没有发生任何的io时间 那么将会下发用户时间
            //将channel空闲超时的事件下发给其他handler进行处理 idlestateevent传给下一个handler进行处理
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    /**
     * 当channel底层selectionkey绑定到eventloop的selector上之后 会执行性channelregistered方法
     * @param ctx channelhandlercontext对象
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired.  If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {
            reading = true;
            firstReaderIdleEvent = firstAllIdleEvent = true;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if ((readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) && reading) {
            lastReadTime = System.nanoTime();
            reading = false;
        }
        ctx.fireChannelReadComplete();
    }

    /**
     *  向channel的输出缓冲区写入数据的方法
     * @param ctx channelhandlerContext对象
     * @param msg 消息数据对象
     * @param promise 向channel写入数据的异步操作对象
     * @throws Exception
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Allow writing with void promise if handler is only configured for read timeout events.
        if (writerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {//如果写的超时心跳时间 如果所有空闲的超时心跳时间大于0
            //向写的异步操作添加一个回调 等待写完成之后 执行回调方法
            promise.addListener(writeListener);
        }
        ctx.write(msg, promise);
    }

    /**
     * 初始化channelhandlerContext对象
     * @param ctx 被初始化的channelhandlerContext对象
     */
    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
        case 1:
        case 2:
            return;
        }

        //state 初始值为0 赋值为1
        state = 1;

        //获取channelhandlerContext的执行器
        EventExecutor loop = ctx.executor();

        //把上一次读时间、写时间赋值为当前时间
        lastReadTime = lastWriteTime = System.nanoTime();
        if (readerIdleTimeNanos > 0) {//初始值为0
            readerIdleTimeout = loop.schedule(
                    new ReaderIdleTimeoutTask(ctx),
                    readerIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
        if (writerIdleTimeNanos > 0) {//初始值为0
            writerIdleTimeout = loop.schedule(
                    new WriterIdleTimeoutTask(ctx),
                    writerIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
        if (allIdleTimeNanos > 0) {
            allIdleTimeout = loop.schedule(
                    new AllIdleTimeoutTask(ctx),
                    allIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
    }

    private void destroy() {
        state = 2;

        if (readerIdleTimeout != null) {
            readerIdleTimeout.cancel(false);
            readerIdleTimeout = null;
        }
        if (writerIdleTimeout != null) {
            writerIdleTimeout.cancel(false);
            writerIdleTimeout = null;
        }
        if (allIdleTimeout != null) {
            allIdleTimeout.cancel(false);
            allIdleTimeout = null;
        }
    }

    /**
     * Is called when an {@link IdleStateEvent} should be fired. This implementation calls
     * {@link ChannelHandlerContext#fireUserEventTriggered(Object)}.
     */
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        //触发用户时间
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * 创建一个channel idle event事件对象
     * @param state 空闲的类型
     * @param first 是否为channel第一次出现空闲
     * @return
     */
    protected IdleStateEvent newIdleStateEvent(IdleState state, boolean first) {
        switch (state) {
            case ALL_IDLE://在指定的超时时间内 channel没有发生任何的读或者写的io事件
                return first ? IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT : IdleStateEvent.ALL_IDLE_STATE_EVENT;
            case READER_IDLE://在指定的超时时间内 channel没有发生任何的读io事件
                return first ? IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT : IdleStateEvent.READER_IDLE_STATE_EVENT;
            case WRITER_IDLE://在指定的超时时间内 channel没有发生任何的写io事件
                return first ? IdleStateEvent.FIRST_WRITER_IDLE_STATE_EVENT : IdleStateEvent.WRITER_IDLE_STATE_EVENT;
            default:
                throw new Error();
        }
    }

    private final class ReaderIdleTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        ReaderIdleTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            long nextDelay = readerIdleTimeNanos;
            if (!reading) {
                nextDelay -= System.nanoTime() - lastReadTime;
            }

            if (nextDelay <= 0) {
                // Reader is idle - set a new timeout and notify the callback.
                readerIdleTimeout =
                    ctx.executor().schedule(this, readerIdleTimeNanos, TimeUnit.NANOSECONDS);
                try {
                    IdleStateEvent event = newIdleStateEvent(IdleState.READER_IDLE, firstReaderIdleEvent);
                    if (firstReaderIdleEvent) {
                        firstReaderIdleEvent = false;
                    }

                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Read occurred before the timeout - set a new timeout with shorter delay.
                readerIdleTimeout = ctx.executor().schedule(this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }

    private final class WriterIdleTimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        WriterIdleTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            long lastWriteTime = IdleStateHandler.this.lastWriteTime;
            long nextDelay = writerIdleTimeNanos - (System.nanoTime() - lastWriteTime);
            if (nextDelay <= 0) {
                // Writer is idle - set a new timeout and notify the callback.
                writerIdleTimeout = ctx.executor().schedule(
                        this, writerIdleTimeNanos, TimeUnit.NANOSECONDS);
                try {
                    IdleStateEvent event = newIdleStateEvent(IdleState.WRITER_IDLE, firstWriterIdleEvent);
                    if (firstWriterIdleEvent) {
                        firstWriterIdleEvent = false;
                    }

                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Write occurred before the timeout - set a new timeout with shorter delay.
                writerIdleTimeout = ctx.executor().schedule(this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * 所有空闲的超时任务
     */
    private final class AllIdleTimeoutTask implements Runnable {

        /**
         * idlehandlerContext对象
         */
        private final ChannelHandlerContext ctx;

        AllIdleTimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        /**
         * channel jdk底层的selectionkey注册到e没ventloop的selector上之后120秒之后执行这个方法
         */
        @Override
        public void run() {
            //idlehandlercontext的channel没有打开 直接return
            if (!ctx.channel().isOpen()) {
                return;
            }

            //超时时间120 * 1000000微妙
            long nextDelay = allIdleTimeNanos;
            if (!reading) {
                nextDelay -= System.nanoTime() - Math.max(lastReadTime, lastWriteTime);
            }

            //期间handler处理任何channel的读写的io事件
            if (nextDelay <= 0) {
                //再次添加一个定时任务 120秒之后 再次这个run方法
                allIdleTimeout = ctx.executor().schedule(
                        this, allIdleTimeNanos, TimeUnit.NANOSECONDS);
                try {
                    //实例化一个channel的 空闲状态事件对象
                    IdleStateEvent event = newIdleStateEvent(IdleState.ALL_IDLE, firstAllIdleEvent);
                    if (firstAllIdleEvent) {
                        firstAllIdleEvent = false;
                    }

                    //如果channel在指定超时时间段内没有发生io时间 处理
                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Either read or write occurred before the timeout - set a new
                // timeout with shorter delay.
                allIdleTimeout = ctx.executor().schedule(this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }
}
