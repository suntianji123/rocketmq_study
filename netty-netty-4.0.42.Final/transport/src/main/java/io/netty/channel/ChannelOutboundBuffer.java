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
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.PromiseNotificationUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 每个channel的出站缓冲区类
 */
public final class ChannelOutboundBuffer {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);

    /**
     * 线程上下文变量
     */
    private static final FastThreadLocal<ByteBuffer[]> NIO_BUFFERS = new FastThreadLocal<ByteBuffer[]>() {
        @Override
        protected ByteBuffer[] initialValue() throws Exception {
            return new ByteBuffer[1024];
        }
    };

    /**
     * 出站缓冲区所属的channel对象
     */
    private final Channel channel;

    // Entry(flushedEntry) --> ... Entry(unflushedEntry) --> ... Entry(tailEntry)
    //
    // 将要被刷新的entry链表的起始节点
    private Entry flushedEntry;
    // 没有被刷新的entry的头部节点
    private Entry unflushedEntry;
    // 链表尾部的实体对象
    private Entry tailEntry;
    // 等待刷新的entry数量
    private int flushed;

    /**
     * 线程上下文变量NIO_BUFFER中的ByteBuffer数组的实际ByteBuffer的数量
     */
    private int nioBufferCount;

    /**
     * 线程上下文变量NIO_BUFFER中的ByteBuffer数组中ByteBuffer总的字节数
     */
    private long nioBufferSize;

    /**
     * 缓冲区刷新失败状态标志
     */
    private boolean inFail;

    /**
     * 输出缓冲区 totalPendingSize 字段更新器
     */
    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER;

    /**
     * 输出缓冲区已经写入的字节大小
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile long totalPendingSize;

    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER;

    /**
     * 不能继续向输出缓冲区写入字节的标志值 0 可写；1 不可泄
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int unwritable;

    /**
     * 下发channel可写状态变更的事件 run方法
     */
    private volatile Runnable fireChannelWritabilityChangedTask;

    static {
        AtomicIntegerFieldUpdater<ChannelOutboundBuffer> unwritableUpdater =
                PlatformDependent.newAtomicIntegerFieldUpdater(ChannelOutboundBuffer.class, "unwritable");
        if (unwritableUpdater == null) {
            unwritableUpdater = AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");
        }
        UNWRITABLE_UPDATER = unwritableUpdater;

        AtomicLongFieldUpdater<ChannelOutboundBuffer> pendingSizeUpdater =
                PlatformDependent.newAtomicLongFieldUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
        if (pendingSizeUpdater == null) {
            pendingSizeUpdater = AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
        }
        TOTAL_PENDING_SIZE_UPDATER = pendingSizeUpdater;
    }

    /**
     * 实例化一个出站缓冲区对象
     * @param channel channel对象
     */
    ChannelOutboundBuffer(AbstractChannel channel) {
        //设置出站缓冲区所属的channel对象
        this.channel = channel;
    }

    /**
     * 向channel的输出缓冲区添加一个消息
     * @param msg 消息对象
     * @param size 消息的大小
     * @param promise 向channel写入数据的异步操作对象
     */
    public void addMessage(Object msg, int size, ChannelPromise promise) {
        //实例化一个实体
        Entry entry = Entry.newInstance(msg, size, total(msg), promise);
        if (tailEntry == null) {//尾部实体为null
            flushedEntry = null; //设置已经被刷新的实体为null
            tailEntry = entry;//设置尾部实体对象
        } else {//如果尾部实体已经存在 则将消息直接插入到链表的尾部
            Entry tail = tailEntry;
            tail.next = entry;
            tailEntry = entry;
        }
        if (unflushedEntry == null) {//如果当前没有没有被刷新的实体
            //设置没有被刷新的实体
            unflushedEntry = entry;
        }

        //增加channel的输出缓冲区的总的数据大小
        incrementPendingOutboundBytes(size, false);
    }

    /**
     * 添实体添加到需要被刷新的缓冲区链表
     */
    public void addFlush() {
        //获取没有被刷新的实体
        Entry entry = unflushedEntry;
        if (entry != null) {//实体不为null
            if (flushedEntry == null) {//如果将要被刷新的实体为null
                // there is no flushedEntry yet, so start with the entry
                //设置将要被刷新的实体为没有被刷新的实体
                flushedEntry = entry;
            }
            do {
                //增加已经刷新的实体数量
                flushed ++;
                if (!entry.promise.setUncancellable()) {//写入到channel的异步操作对象设置取消失败
                    // Was cancelled so make sure we free up memory and notify about the freed bytes
                    //取消当前实体
                    int pending = entry.cancel();
                    decrementPendingOutboundBytes(pending, false, true);
                }
                entry = entry.next;
            } while (entry != null);

            // All flushed so reset unflushedEntry
            unflushedEntry = null;
        }
    }

    /**
     * 增加输出缓冲区的字节数
     * @param size 需要增加的size大小
     */
    void incrementPendingOutboundBytes(long size) {
        incrementPendingOutboundBytes(size, true);
    }

    /**
     * 增加输出缓冲区的字节数
     * @param size 增加的大小
     * @param invokeLater 是否稍后执行
     */
    private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
        if (size == 0) {//如果需要增加的值为0 直接返回
            return;
        }

        //增加输出缓冲区的totalPendingSize的值 获取输出缓冲区已经存在的字节数
        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);

        //如果一次向channel的输出缓冲区写入的字节数超过了channel指定的最大字节数 默认64M
        if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
            //设置不能向输出缓冲区写入字节
            setUnwritable(invokeLater);
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void decrementPendingOutboundBytes(long size) {
        decrementPendingOutboundBytes(size, true, true);
    }

    /**
     * 减少输出缓冲区的累计字节数
     * @param size 减少的数量
     * @param invokeLater 如果累计字节数低于channel配置的低水位字节数 是否稍后下发channel的可写状态变更事件
     * @param notifyWritability 是否通知可写状态变更
     */
    private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean notifyWritability) {
        if (size == 0) {//减少的字节数
            return;
        }

        //更新总的字节数
        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
        if (notifyWritability && newWriteBufferSize < channel.config().getWriteBufferLowWaterMark()) {
            setWritable(invokeLater);
        }
    }

    /**
     * 获取消息总的字节数
     * @param msg 消息对象
     * @return
     */
    private static long total(Object msg) {
        if (msg instanceof ByteBuf) {//如果是ByteBuf类型 返回可读字节数
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {//如果是文件类型 返回count
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {//如果是ByteBuf持有者类型  返回可读字节数
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    /**
     * 当前正在等待写入到chanel的entry链表的第一个节点
     * @return
     */
    public Object current() {
        //获取第一个节点
        Entry entry = flushedEntry;
        if (entry == null) {
            return null;
        }

        //获取实体的消息对象
        return entry.msg;
    }

    /**
     * Notify the {@link ChannelPromise} of the current message about writing progress.
     */
    public void progress(long amount) {
        Entry e = flushedEntry;
        assert e != null;
        ChannelPromise p = e.promise;
        if (p instanceof ChannelProgressivePromise) {
            long progress = e.progress + amount;
            e.progress = progress;
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
        }
    }

    /**
     * 删除当前正在等待刷新的entry对象
     * @return
     */
    public boolean remove() {
        //获取等待刷新的entry对象
        Entry e = flushedEntry;
        if (e == null) {
            //清理NIO_BUBBER数组
            clearNioBuffers();
            return false;
        }

        //获取消息对象
        Object msg = e.msg;

        //获取异步操作对象
        ChannelPromise promise = e.promise;

        //获取消息大小
        int size = e.pendingSize;

        //从链表中移除当前entry
        removeEntry(e);

        if (!e.cancelled) {//如果entry没有设置已经取消状态
            // only release message, notify and decrement if it was not canceled before.
            //释放消息
            ReferenceCountUtil.safeRelease(msg);
            //设置异步操作成功
            safeSuccess(promise);
            //减小输出缓冲区字节大小
            decrementPendingOutboundBytes(size, false, true);
        }

        // 回收entry
        e.recycle();

        return true;
    }

    /**
     * Will remove the current message, mark its {@link ChannelPromise} as failure using the given {@link Throwable}
     * and return {@code true}. If no   flushed message exists at the time this method is called it will return
     * {@code false} to signal that no more messages are ready to be handled.
     */
    public boolean remove(Throwable cause) {
        return remove0(cause, true);
    }

    /**
     * 移除所有等待刷新的entry
     * @param cause 移除原因
     * @param notifyWritability 是否下发channel可写状态变更事件
     * @return 返回是否移除过等待刷新的entry节点
     */
    private boolean remove0(Throwable cause, boolean notifyWritability) {
        //获取等待刷新的entry的起始节点
        Entry e = flushedEntry;
        if (e == null) {//没有等待刷新的entry链表
            //设置线程上下文变量NIO_BYTEBUFFER数组的长度为0 清理其中的ByteBuffer
            clearNioBuffers();
            return false;
        }

        //获取被entry包装的消息对象
        Object msg = e.msg;

        //获取写入到channel的异步操作对象
        ChannelPromise promise = e.promise;

        //获取消息对象的字节大小
        int size = e.pendingSize;


        //删除entry
        removeEntry(e);

        if (!e.cancelled) {//如果没有设置取消
            // 释放消息对象
            ReferenceCountUtil.safeRelease(msg);

            //设置entry的写入channel的异步操作结果为失败
            safeFail(promise, cause);

            //减少输出缓冲区对象总的消息大小
            decrementPendingOutboundBytes(size, false, notifyWritability);
        }

        // 回收entry实体
        e.recycle();

        return true;
    }

    /**
     * 从entry链表中删除某个entry
     * @param e entry对象
     */
    private void removeEntry(Entry e) {
        if (-- flushed == 0) {//减小将要被刷新的entry的数量
            // 设置等待刷新的entry为null
            flushedEntry = null;
            if (e == tailEntry) {//如果删除的尾部entry
                tailEntry = null;//设置尾部entry
                unflushedEntry = null;
            }
        } else {
            //设置下一个entry为等待刷新的entry对象
            flushedEntry = e.next;
        }
    }

    /**
     * 减少已经写入到channel的字节数
     * @param writtenBytes 减少的字节数
     */
    public void removeBytes(long writtenBytes) {
        for (;;) {

            //获取当前刷新的Entry消息
            Object msg = current();
            if (!(msg instanceof ByteBuf)) {
                assert writtenBytes == 0;
                break;
            }

            final ByteBuf buf = (ByteBuf) msg;
            final int readerIndex = buf.readerIndex();
            final int readableBytes = buf.writerIndex() - readerIndex;

            if (readableBytes <= writtenBytes) {
                if (writtenBytes != 0) {
                    progress(readableBytes);
                    writtenBytes -= readableBytes;
                }
                remove();
            } else { // readableBytes > writtenBytes
                if (writtenBytes != 0) {
                    buf.readerIndex(readerIndex + (int) writtenBytes);
                    progress(writtenBytes);
                }
                break;
            }
        }
        clearNioBuffers();
    }

    /**
     * 清理线程上下文NIO_BYTEBUFFER字节数组的元素
     */
    private void clearNioBuffers() {
        //如果数组中存在不为null的ByteBuffer对象
        int count = nioBufferCount;
        if (count > 0) {
            //设置数组中有效的ByteBuffer数量为0
            nioBufferCount = 0;
            //填充数组为null
            Arrays.fill(NIO_BUFFERS.get(), 0, count, null);
        }
    }

    /**
     * 获取nioBuffers
     * @return
     */
    public ByteBuffer[] nioBuffers() {
        //已经写入到NIO_BUFFER数组中的字节数
        long nioBufferSize = 0;

        //需要使用的ByteBuffer数量
        int nioBufferCount = 0;
        final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();

        //从线程上下文中获取ByteBuffer[]数组
        ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);

        //当前第一个等待写入的entry对象
        Entry entry = flushedEntry;
        while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {//如果entry是等待写入的entry 并且entry的msg是ByteBuf类型
            if (!entry.cancelled) {//entry没有被取消
                //获取消息对象
                ByteBuf buf = (ByteBuf) entry.msg;
                //获取readerIndex
                final int readerIndex = buf.readerIndex();

                //获取可读字节数
                final int readableBytes = buf.writerIndex() - readerIndex;

                if (readableBytes > 0) {//可读字节数大于0
                    if (Integer.MAX_VALUE - readableBytes < nioBufferSize) {
                        // If the nioBufferSize + readableBytes will overflow an Integer we stop populate the
                        // ByteBuffer array. This is done as bsd/osx don't allow to write more bytes then
                        // Integer.MAX_VALUE with one writev(...) call and so will return 'EINVAL', which will
                        // raise an IOException. On Linux it may work depending on the
                        // architecture and kernel but to be safe we also enforce the limit here.
                        // This said writing more the Integer.MAX_VALUE is not a good idea anyway.
                        //
                        // See also:
                        // - https://www.freebsd.org/cgi/man.cgi?query=write&sektion=2
                        // - http://linux.die.net/man/2/writev
                        break;
                    }

                    //增加累计写入的字节数
                    nioBufferSize += readableBytes;


                    int count = entry.count;
                    if (count == -1) {
                        //设置entry的msg中占有ByteBuffer的数量
                        entry.count = count =  buf.nioBufferCount();
                    }

                    //需要的的ByteBuffer对象的数量
                    int neededSpace = nioBufferCount + count;
                    if (neededSpace > nioBuffers.length) {
                        //扩容ByteBuffer[]数组对象
                        nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                        //设置到上下文变量
                        NIO_BUFFERS.set(threadLocalMap, nioBuffers);
                    }
                    if (count == 1) {
                        //获取ByteBuffer对象
                        ByteBuffer nioBuf = entry.buf;
                        if (nioBuf == null) {
                            //设置entry的buf对象
                            entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                        }

                        //将ByteBuffer放入nioBuffers数组
                        nioBuffers[nioBufferCount ++] = nioBuf;
                    } else {
                        ByteBuffer[] nioBufs = entry.bufs;
                        if (nioBufs == null) {
                            //设置entry的bytebuffer数组
                            entry.bufs = nioBufs = buf.nioBuffers();
                        }

                        //向bytebuffer数组中下入entry msg低层的Bytebuffer数组
                        nioBufferCount = fillBufferArray(nioBufs, nioBuffers, nioBufferCount);
                    }
                }
            }
            entry = entry.next;
        }

        //设置线程上下文变量NIO_BYTEBUFFER中总的ByteBuffer的数量
        this.nioBufferCount = nioBufferCount;

        //设置线程上下文变量NIO_BYTEBUFFER中的所有的ByteBuffer总的字节数
        this.nioBufferSize = nioBufferSize;

        //返回ByteBuffer数组
        return nioBuffers;
    }

    /**
     * 向某个ByteBuffer[]数组填充另一个数组元素
     * @param nioBufs 需要增加ByteBuffer对象的数组
     * @param nioBuffers 被添加的ByteBuffer对象数组
     * @param nioBufferCount 当前ByteBuffer数组写入的起始下标
     * @return 返回新的下标
     */
    private static int fillBufferArray(ByteBuffer[] nioBufs, ByteBuffer[] nioBuffers, int nioBufferCount) {
        for (ByteBuffer nioBuf: nioBufs) {
            if (nioBuf == null) {
                break;
            }
            nioBuffers[nioBufferCount ++] = nioBuf;
        }
        return nioBufferCount;
    }

    /**
     * 扩展NIO_BUFFER byteBuffer数组的容量
     * @param array 需要扩展的数组对象
     * @param neededSpace 扩展之后的容量
     * @param size 当前的容量
     * @return
     */
    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        //获取数量的实际长度
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            //扩容2倍
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        //实例化一个新的数组
        ByteBuffer[] newArray = new ByteBuffer[newCapacity];

        //将原始数组中的元素复制到新的数组
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    /**
     * Returns the number of {@link ByteBuffer} that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public int nioBufferCount() {
        return nioBufferCount;
    }

    /**
     * Returns the number of bytes that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public long nioBufferSize() {
        return nioBufferSize;
    }

    /**
     * Returns {@code true} if and only if {@linkplain #totalPendingWriteBytes() the total number of pending bytes} did
     * not exceed the write watermark of the {@link Channel} and
     * no {@linkplain #setUserDefinedWritability(int, boolean) user-defined writability flag} has been set to
     * {@code false}.
     */
    public boolean isWritable() {
        return unwritable == 0;
    }

    /**
     * Returns {@code true} if and only if the user-defined writability flag at the specified index is set to
     * {@code true}.
     */
    public boolean getUserDefinedWritability(int index) {
        return (unwritable & writabilityMask(index)) == 0;
    }

    /**
     * Sets a user-defined writability flag at the specified index.
     */
    public void setUserDefinedWritability(int index, boolean writable) {
        if (writable) {
            setUserDefinedWritability(index);
        } else {
            clearUserDefinedWritability(index);
        }
    }

    private void setUserDefinedWritability(int index) {
        final int mask = ~writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue & mask;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue != 0 && newValue == 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private void clearUserDefinedWritability(int index) {
        final int mask = writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue | mask;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue == 0 && newValue != 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private static int writabilityMask(int index) {
        if (index < 1 || index > 31) {
            throw new IllegalArgumentException("index: " + index + " (expected: 1~31)");
        }
        return 1 << index;
    }

    private void setWritable(boolean invokeLater) {
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue & ~1;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue != 0 && newValue == 0) {
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    /**
     * 设置channel的输出缓冲区不能继续 输出缓冲区的已经累计的数据超过了 channel配置的最高水位值64M
     * @param invokeLater
     */
    private void setUnwritable(boolean invokeLater) {
        for (;;) {
            //获取老的不能写入的标志值
            final int oldValue = unwritable;
            //新的状态值
            final int newValue = oldValue | 1;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                if (oldValue == 0 && newValue != 0) {
                    //下发 channel可写的状态变化的消息
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    /**
     * 下发通道可写状态变更的消息
     * @param invokeLater 是否为稍后执行
     */
    private void fireChannelWritabilityChanged(boolean invokeLater) {
        //获取chanel的pipeline
        final ChannelPipeline pipeline = channel.pipeline();
        if (invokeLater) {//如果是稍后执行
            Runnable task = fireChannelWritabilityChangedTask;
            if (task == null) {
                fireChannelWritabilityChangedTask = task = new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelWritabilityChanged();
                    }
                };
            }
            channel.eventLoop().execute(task);
        } else {
            pipeline.fireChannelWritabilityChanged();
        }
    }

    /**
     * Returns the number of flushed messages in this {@link ChannelOutboundBuffer}.
     */
    public int size() {
        return flushed;
    }

    /**
     * 判断输出缓冲区是否没有等待被刷新的entry对象
     * @return
     */
    public boolean isEmpty() {
        return flushed == 0;
    }

    /**
     * 设置输出缓冲区对象刷新失败
     * @param cause 失败原因
     * @param notify 是否通知
     */
    void failFlushed(Throwable cause, boolean notify) {
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {//如果已经设置过失败 则直接返回
            return;
        }

        //删除所有等待刷新的entry
        try {
            //设置缓冲区处理失败装填
            inFail = true;
            for (;;) {
                if (!remove0(cause, notify)) {
                    break;
                }
            }
        } finally {
            inFail = false;
        }
    }

    void close(final ClosedChannelException cause) {
        if (inFail) {
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause);
                }
            });
            return;
        }

        inFail = true;

        if (channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        try {
            Entry e = unflushedEntry;
            while (e != null) {
                // Just decrease; do not trigger any events via decrementPendingOutboundBytes()
                int size = e.pendingSize;
                TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);

                if (!e.cancelled) {
                    ReferenceCountUtil.safeRelease(e.msg);
                    safeFail(e.promise, cause);
                }
                e = e.recycleAndGetNext();
            }
        } finally {
            inFail = false;
        }
        clearNioBuffers();
    }

    private static void safeSuccess(ChannelPromise promise) {
        if (!(promise instanceof VoidChannelPromise)) {
            PromiseNotificationUtil.trySuccess(promise, null, logger);
        }
    }

    private static void safeFail(ChannelPromise promise, Throwable cause) {
        if (!(promise instanceof VoidChannelPromise)) {
            PromiseNotificationUtil.tryFailure(promise, cause, logger);
        }
    }

    @Deprecated
    public void recycle() {
        // NOOP
    }

    public long totalPendingWriteBytes() {
        return totalPendingSize;
    }

    /**
     * Get how many bytes can be written until {@link #isWritable()} returns {@code false}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code false} then 0.
     */
    public long bytesBeforeUnwritable() {
        long bytes = channel.config().getWriteBufferHighWaterMark() - totalPendingSize;
        // If bytes is negative we know we are not writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? bytes : 0;
        }
        return 0;
    }

    /**
     * Get how many bytes must be drained from the underlying buffer until {@link #isWritable()} returns {@code true}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code true} then 0.
     */
    public long bytesBeforeWritable() {
        long bytes = totalPendingSize - channel.config().getWriteBufferLowWaterMark();
        // If bytes is negative we know we are writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? 0 : bytes;
        }
        return 0;
    }

    /**
     * Call {@link MessageProcessor#processMessage(Object)} for each flushed message
     * in this {@link ChannelOutboundBuffer} until {@link MessageProcessor#processMessage(Object)}
     * returns {@code false} or there are no more flushed messages to process.
     */
    public void forEachFlushedMessage(MessageProcessor processor) throws Exception {
        if (processor == null) {
            throw new NullPointerException("processor");
        }

        Entry entry = flushedEntry;
        if (entry == null) {
            return;
        }

        do {
            if (!entry.cancelled) {
                if (!processor.processMessage(entry.msg)) {
                    return;
                }
            }
            entry = entry.next;
        } while (isFlushedEntry(entry));
    }

    /**
     *判断entry是否为等待写入到channel的entry
     * @param e entry对象
     * @return
     */
    private boolean isFlushedEntry(Entry e) {
        return e != null && e != unflushedEntry;
    }

    public interface MessageProcessor {
        /**
         * Will be called for each flushed message until it either there are no more flushed messages or this
         * method returns {@code false}.
         */
        boolean processMessage(Object msg) throws Exception;
    }

    /**
     * 包装写入到channel消息的实体类
     */
    static final class Entry {
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
            @Override
            protected Entry newObject(Handle handle) {
                return new Entry(handle);
            }
        };

        /**
         * 处理回收的handle
         */
        private final Handle handle;

        /**
         * 位于链表的下一个实体
         */
        Entry next;

        /**
         * 写入channel的消息对象
         */
        Object msg;

        /**
         *msg消息对象持有的jdk低层的ByteBuffer列表对象
         */
        ByteBuffer[] bufs;

        /**
         * msg消息对象持有的jdk低层的ByteBuffer对象
         */
        ByteBuffer buf;

        /**
         * 将msg写入到channel的异步操作对象
         */
        ChannelPromise promise;

        /**
         * 当前进度值
         */
        long progress;

        /**
         * 总的消息大小值
         */
        long total;

        /**
         * msg消息的字节数
         */
        int pendingSize;

        /**
         * msg中ByteBuffer的数量
         */
        int count = -1;

        /**
         * 写入到channel的msg包装的实体对象是否已经被取消
         */
        boolean cancelled;

        private Entry(Handle handle) {
            this.handle = handle;
        }

        /**
         * 实例化一个实体对象
         * @param msg 消息对象
         * @param size 消息对象的字节大小
         * @param total 消息对象的总的字节大小
         * @param promise 写入到channel的输出缓冲区的异步操作对象
         * @return
         */
        static Entry newInstance(Object msg, int size, long total, ChannelPromise promise) {
            //从回收站获取一个实体
            Entry entry = RECYCLER.get();
            //设置实体的msg
            entry.msg = msg;
            //设置实体的消息大小
            entry.pendingSize = size;
            //设置实体的总的大小
            entry.total = total;
            //设置写入到channel的异步操作对象
            entry.promise = promise;
            return entry;
        }

        /**
         * 取消某个实体
         * @return
         */
        int cancel() {
            if (!cancelled) {//r没有被取消
                //设置取消标志
                cancelled = true;
                //获取消息的字节大小
                int pSize = pendingSize;

                // release message and replace with an empty buffer
                //释放消息
                ReferenceCountUtil.safeRelease(msg);
                //将消息设置为长度为0的ByteBuf对象
                msg = Unpooled.EMPTY_BUFFER;

                pendingSize = 0;
                total = 0;
                progress = 0;
                bufs = null;
                buf = null;
                //返回消息的字节数
                return pSize;
            }
            return 0;
        }

        void recycle() {
            next = null;
            bufs = null;
            buf = null;
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = -1;
            cancelled = false;
            RECYCLER.recycle(this, handle);
        }

        Entry recycleAndGetNext() {
            Entry next = this.next;
            recycle();
            return next;
        }
    }
}
