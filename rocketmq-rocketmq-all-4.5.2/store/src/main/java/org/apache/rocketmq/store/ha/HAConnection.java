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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 与从站建立的高可用连接
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 高可用服务
     */
    private final HAService haService;

    /**
     * 与从站建立的连接
     */
    private final SocketChannel socketChannel;

    /**
     * 从站地址
     */
    private final String clientAddr;

    /**
     * 向socketchannel写数据的服务
     */
    private WriteSocketService writeSocketService;

    /**
     * 从socketchannel读数据的服务
     */
    private ReadSocketService readSocketService;

    /**
     * 给从站请求的偏移量（从站当前同步到的偏移量）
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * 从站报告给主站自己当前同步到的偏移量
     */
    private volatile long slaveAckOffset = -1;

    /**
     * 高可用连接对象
     * @param haService 主站所在的高可用服务
     * @param socketChannel 与从站建立的socketchannel连接对象
     * @throws IOException
     */
    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        //设置高可用服务
        this.haService = haService;
        //设置与从站建立的socketChannel对象
        this.socketChannel = socketChannel;

        //设置从站地址
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        //设置通道非阻塞
        this.socketChannel.configureBlocking(false);

        this.socketChannel.socket().setSoLinger(false, -1);

        //没有延迟
        this.socketChannel.socket().setTcpNoDelay(true);
        //缓存区可缓存的最大字节数 64M
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        //向缓存区可写的最大字节数
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        //设置向socketChannal写入数据的服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        //设置从socketChannel读取数据的服务
        this.readSocketService = new ReadSocketService(this.socketChannel);
        //增加haService的与从站建立的连接数
        this.haService.getConnectionCount().incrementAndGet();
    }

    /**
     * 启动高可用连接
     */
    public void start() {
        //启动从socketChannel读取数据的服务
        this.readSocketService.start();
        //启动向socketChannel写入数据的服务
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;

        /**
         * 读取与从站建立SocketChannel中字节的临时缓存区
         */
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * 已经处理到byteBufferRead字节的位置 每8个字节为一个long数为从站commitlog文件系统的最大偏移量
         */
        private int processPosition = 0;

        /**
         * 上一次从与从站的SocketChannel读取的数据时间
         */
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        /**
         * 处理从站socketChannel io事件逻辑
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {//服务没有关闭
                try {
                    //轮训注册到selector上的channel的io事件
                    this.selector.select(1000);

                    //读取从站写过来的字节
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    //获取最后一次读取从站commitlog文件系统偏移量的时间
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    //大于心跳时间 说明已经发送过心跳
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            //关闭当前服务
            this.makeStop();

            //关闭向SocketChannel中写数据的服务
            writeSocketService.makeStop();

            //移除与从站建立的SocketChannel连接
            haService.removeConnection(HAConnection.this);

            //减少连接数量
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            //取消SocketChannel注册到selector上的selectionKey
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                //关闭轮训器
                this.selector.close();
                //关闭socketchannel
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理从站写入到socketChannel的事件
         * @return
         */
        private boolean processReadEvent() {
            //读到字节数为0的次数
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {//没有字节可读
                //将读改为写
                this.byteBufferRead.flip();
                //已经处理到的位置为0
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {//还有剩余可写空间
                try {
                    //读取SocketChannel中的字节 写入到缓存区
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {//从站有发送数据过来
                        //设置读取数据为0的次数为0
                        readSizeZeroTimes = 0;
                        //设置最后一次从从站SocketChannel读取数据的时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {//读取的字节数超过了8个
                            //获取第一条消息的终止位置
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            //从第一条消息的起始开始 读取一个long数为从站的commitlog文件系统的最大偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            //设置已经处理到的位置
                            this.processPosition = pos;
                            //设置从站最大偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                //设置从站请求同步的起始偏移量
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            //从站同步数据完成 告诉其他线程继续执行
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {//超过3次 跳出循环
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 向从站的SocketChannel写入数据的服务
     */
    class WriteSocketService extends ServiceThread {
        /**
         * SocketChannel的轮训器 轮训通道的IO事件
         */
        private final Selector selector;

        /**
         * 从站的SocketChannel连接
         */
        private final SocketChannel socketChannel;

        private final int headerSize = 8 + 4;

        /**
         * 告诉从站当前主站的commitlog文件系统的偏移量的byteBuffer对象 12个字节 前8个字节表示当前主站commitlog的偏移量 剩余4个字节写入同步消息的长度
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        /**
         * 下一次和从站的commitlog文件同步数据的起始偏移量
         */
        private long nextTransferFromWhere = -1;
        private SelectMappedBufferResult selectMappedBufferResult;

        /**
         * 最后向从站内的SocketChannel写数据是否已经完成
         */
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 实例化一个向从站SocketChannel写数据的服务
         * @param socketChannel 主站的高可用服务与从站建立的SocketChannel连接
         * @throws IOException
         */
        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            //设置轮训SocketChannel io事件的轮训器
            this.selector = RemotingUtil.openSelector();
            //设置与从站建立的SocketChannel连接对象
            this.socketChannel = socketChannel;
            //将SocketChannel注册到轮训器
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            //设置服务的工作线程为守护线程
            this.setDaemon(true);
        }

        /**
         * 向从站的SocketChannel写数据的运行方法体
         */
        @Override
        public void run() {
            //打印日志
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {//服务没有关闭
                try {
                    //轮训注册channel的io事件 超时时间为1秒
                    this.selector.select(1000);

                    if (-1 == HAConnection.this.slaveRequestOffset) {//从站启动的时候 告诉了主站本地的commitlog文件系统的偏移量之后
                        Thread.sleep(10);
                        continue;
                    }

                    if (-1 == this.nextTransferFromWhere) {//从站从来没有同步过数据
                        if (0 == HAConnection.this.slaveRequestOffset) {//第一次给从站发送同步数据请求
                            //主站的commitlog文件系统的最大偏移量
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();

                            //第一和从站同步数据 只同步主站最后一个commitlog文件起始偏移量
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            //设置下一次同步的起始偏移量
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //设置下一次同步的起始偏移量为从站已经同步到的偏移量
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    if (this.lastWriteOver) {//最后一次向从站的SocketChannel写数据已经完成
                        //获取距离上次向SocketChannel写数据的时间
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {//大于心跳时间 说明需要继续通知从站主站的当前commitlog文件系统的偏移量

                            // 向缓存区中写入当前主站的commitlog文件系统的偏移量
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            //将当前commitlog的偏移量 以及消息的长度 同步的消息写入到socketchannel 设置最后一次向socketchannel中写入数据结束
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    //获取下一次需要同步给从站的commitlog中的数据
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {//主站commitlog存在数据
                        //获取字节包大小
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {//数据包长度超过了32K
                            //单次同步只能同步32K
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        //记录本次同步的起始偏移量
                        long thisOffset = this.nextTransferFromWhere;
                        //设置下一次同步的起始偏移量
                        this.nextTransferFromWhere += size;

                        //设置缓存区限制
                        selectResult.getByteBuffer().limit(size);
                        //获取同步数据选择的commitlog数据
                        this.selectMappedBufferResult = selectResult;

                        // 创建数据头8字节偏移量 4字节数据包大小 剩下字节数据
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        //将数据写入到SocketChannel 同步给从站
                        this.lastWriteOver = this.transferData();
                    } else {
                        //主站没有数据 使获取结果的线程等待100毫秒
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            //服务关闭
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            //释放选择commitlog文件系统的结果
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            //停止当前服务
            this.makeStop();

            //停止从SocketChannel读取数据的服务
            readSocketService.makeStop();

            //移除与从站建立的SocketChannel连接
            haService.removeConnection(HAConnection.this);

            //取消当前SocketChannel注册到selector的SelectionKey
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                //关闭轮训器
                this.selector.close();
                //关闭socketChannel
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 将主站commitlog的偏移量 以及同步的数据的写入SocketChannel
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            //向socketChannel写入0 的次数
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {//缓存区可写
                //写入当前主站的commitlog文件系统的偏移量 消息的长度为0
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {//写入的字节数量大于0
                    //设置写入0的次数为0
                    writeSizeZeroTimes = 0;
                    //设置上一次向socketchannel写入数据的时间
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {//写入数据为0的次数大于等于3次  跳出循环
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {//没有找到selectMappedBufferResult直接返回
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
