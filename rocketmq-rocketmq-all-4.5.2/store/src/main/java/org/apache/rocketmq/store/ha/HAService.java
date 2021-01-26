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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

/**
 * 高可用服务
 * 主要用于从站同步主站数据
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 已经和主站建立连接的数量
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * 已经和主站建立连接的连接列表
     */
    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * 接收从站socket请求的服务对象
     */
    private final AcceptSocketService acceptSocketService;

    /**
     *消息存储对象
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 当前线程和其他线程同步同步的锁对象
     */
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 从站已经同步主站commitlog系统最大的偏移量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 主从站组传输数据的服务
     */
    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    /**
     * 实例化一个高可用服务对象
     * @param defaultMessageStore 消息存储
     * @throws IOException
     */
    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        //设置消息存储
        this.defaultMessageStore = defaultMessageStore;
        //设置接收从站channel连接的服务
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());

        //设置主从数据传输服务
        this.groupTransferService = new GroupTransferService();
        //打开一个selector轮训器
        this.haClient = new HAClient();
    }

    /**
     * 更新主站地址
     * @param newAddr 主站地址
     */
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {//更新主站地址
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    /**
     * 向ha服务中添加一个同步数据的请求
     * @param request 同步数据的请求对象
     */
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * 主站是否可以向从站同步commitlog文件系统的数据
     * @param masterPutWhere 主站向从站同步commitlog文件系统系统的终止偏移量
     * @return
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        //和主站建立连接的从站数量必须大于0
        boolean result = this.connectionCount.get() > 0;

        //单次同步数据的字节数不能超过256M
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 通知传输数据
     * @param offset 从站当前commitlog文件系统的偏移量
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {//某个从站同步的偏移量 超过了同步的最大偏移量
            //设置已经同步到的最大偏移量
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                //告诉其他线程 从站同步数据完成
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        //接收从站channel连接的服务开始接收
        //打开selector 设置serverSocketChannel的监听端口 将selector注册到哦serverSocketChannel
        this.acceptSocketService.beginAccept();
        //启动工作线程
        this.acceptSocketService.start();
        //启动主从数据传输服务
        this.groupTransferService.start();
        //启动haclient对象
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    /**
     * 移除与从站建立的SocketChannel连接
     * @param conn  连接
     */
    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * 接收从站channel连接的服务
     */
    class AcceptSocketService extends ServiceThread {
        //监听socket连接的服务器地址
        private final SocketAddress socketAddressListen;
        //与监听地址建立的serverScoketChannel对象
        private ServerSocketChannel serverSocketChannel;
        //轮训serverSocketChannel状态的轮训器
        private Selector selector;

        /**
         * 实例化一个接收从站SocketChannel连接的对象
         * @param port 监听端口
         */
        public AcceptSocketService(final int port) {
            //设置监听对象
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * 接收从站channel连接的服务开始接收
         * @throws Exception
         */
        public void beginAccept() throws Exception {
            //打开一个ServerSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            //打开一个轮训serverSocketChannel的轮训器
            this.selector = RemotingUtil.openSelector();
            //设置重复利用地址
            this.serverSocketChannel.socket().setReuseAddress(true);
            //绑定serverSocketChannel的端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            //非阻塞
            this.serverSocketChannel.configureBlocking(false);
            //给serverSocketChannel注册轮训器
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 接收从站连接请求的线程执行方法体
         * {@inheritDoc}
         */
        @Override
        public void run() {
            //记录日志
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {//服务没有关闭
                try {

                    //轮训一次 最大时间为1000毫秒
                    this.selector.select(1000);
                    //获取轮训到的SocketChannel所绑定的SelectionKey列表
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {//遍历轮训到SocketChannel所绑定的SelectionKey
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                //获取从站建立的SocketChannel连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                //从站建立的连接不为null
                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        //将连接包装为高可用的连接
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        //启动高可用连接
                                        conn.start();
                                        //向高可用服务中添加一个连接
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 主从站传输数据的服务
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();

        /**
         * 等待向从站SocketChannel写入同步请求数据的列表
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();

        /**
         * 等待从从站SocketChannel读取同步请求数据的列表
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * 向主从站传输数据的服务中添加一个请求
         * @param request
         */
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {//加锁
                //向等待同步从站的数据请求列表中添加一个请求
                this.requestsWrite.add(request);
            }

            //通知线程立刻执行
            if (hasNotified.compareAndSet(false, true)) {

                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        /**
         * 交换请求
         */
        private void swapRequests() {
            //获取请求列表
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            //将读请求赋值给写请求列表
            this.requestsWrite = this.requestsRead;
            //将写请求赋值给读请求
            this.requestsRead = tmp;
        }

        /**
         * 传输数据逻辑
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {//等待读取从站的同步请求加锁
                if (!this.requestsRead.isEmpty()) {//从站主动发起了同步请求
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        //从站请求的需要同步的偏移量大于已经同步的最大偏移量 才能同步数据给从站
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        for (int i = 0; !transferOK && i < 5; i++) {//尝试5次 每次阻塞1秒 判断是否同步完成 最多5秒
                            //阻塞传输数据的线程1秒
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        //5秒之后 返回结果 继续之前阻塞的主站线程
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        /**
         *  主从站传输数据的服务
         */
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {//服务没有关闭
                try {
                    //阻塞10毫秒 继续执行
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * 当HAService有同步从站请求时候执行
         */
        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * 高可用客户端对象
     */
    class HAClient extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;

        /**
         * 主站高可用服务地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();

        /**
         * 将当前从站的commitlog文件系统的最大偏移量告诉主站高可用服务的临时ByteBuffer缓存区对象
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;

        /**
         * 轮训器
         */
        private Selector selector;

        /**
         * 最后一次向socketChannel写入数据的时间
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 当前从站的commitlog的最大偏移量
         */
        private long currentReportedOffset = 0;

        /**
         * 分发处理主站向socketchannel写入的字节数组的起始位置
         */
        private int dispatchPosition = 0;

        /**
         * 单次SocketChannel读出数据到缓冲区的大小 4M
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * 实例化一个高可用客户端对象
         * @throws IOException
         */
        public HAClient() throws IOException {
            //打开轮训器
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * 更新主站高可用服务器地址
         * @param newAddr 主站高可用服务地址
         */
        public void updateMasterAddress(final String newAddr) {
            //获取之前的服务地址
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                //重新设置主站高可用服务地址
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         *
         * @return
         */
        private boolean isTimeToReportOffset() {
            //获取上一次同步commitlog文件系统的时间
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            //超过了心跳时间
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            //需要发送心跳
            return needHeart;
        }

        /**
         * 将当前从站的commitlog文件系统的最大偏移量告诉高可用服务器
         * @param maxOffset 本地commitlog文件系统的最大偏移量
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            //设置位置
            this.reportOffset.position(0);
            //设置字节长度
            this.reportOffset.limit(8);
            //写入一个long 从站commitlog最大偏移量
            this.reportOffset.putLong(maxOffset);
            //将位置设置到0
            this.reportOffset.position(0);
            //设置最大偏移量为8
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {//遍历3次 向socketChannel中写入最大偏移量
                try {
                    //将本地commitlog的最大偏移量写入socketChannel
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            //更新最后一次向socketChannel写入数据的时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            //已经完全数据写入到socketChannel
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 读取主站向SocketChannel写入的数据
         * @return
         */
        private boolean processReadEvent() {
            //读取数据为0的次数
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {//缓存区有剩余存放空间
                try {
                    //将socketChannel中的字节读入到缓冲区
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {//读取的字节数量大于0
                        //设置读取字节数为0的次数为0
                        readSizeZeroTimes = 0;

                        //将主站写入到SocketChannel中的数据 8个字节主站同步起始偏移量 4个字节消息包长度 剩下字节消息包
                        //将数据写入到从站commitlog文件系统
                        boolean result = this.dispatchReadRequest();
                        if (!result) {//写入到commitlog文件系统失败
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {//重试3次
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 分发主站向socketchannel写入的字节请求
         * @return
         */
        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            //当前从socketchannel向缓存区写入到的位置
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                //12个字节
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {//大于等于消息头的长度
                    //读取当前主站commitlog文件系统的最大偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    //读取将要同步消息包的总的字节长度
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    //获取当前从站的偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {//主从站的偏移量没有同步
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {//读取到了消息
                        //实例化一个数组 存放消息包
                        byte[] bodyData = new byte[bodySize];
                        //从消息包的起始位置开始
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        //读取消息包到缓存区
                        this.byteBufferRead.get(bodyData);

                        //从masterPhyOffset位置开始 将数据写入到commitlog文件系统
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        //重置byteBufferRead的位置
                        this.byteBufferRead.position(readSocketPos);
                        //已经分发消息的位置
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        //将从站本地当前同步的偏移量报告给主站
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    //重新分配缓存区的大小
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        /**
         * 将从站的commitlog的偏移量告诉给主站
         * @return
         */
        private boolean reportSlaveMaxOffsetPlus() {
            //结果
            boolean result = true;
            //获取本地commitlog文件系统的偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {//从站本地的commitlog实际的commitlog文件的偏移量大于记录的偏移量
                //重新记录的偏移量为本地commitlog的最大偏移量
                this.currentReportedOffset = currentPhyOffset;
                //将同步结果报告给主站
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {//报告失败
                    //关闭主站
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            //返回报告结果
            return result;
        }

        /**
         *连接到主站
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {//还没有与主站建立socketChannel连接
                //获取主站地址
                String addr = this.masterAddress.get();
                if (addr != null) {
                    //获取主站地址 连接远程服务器 返回channel连接
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {//主站地址不为null
                        //连接远程服务器
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {//将channel注册到轮训器 轮训channel的io事件
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                //当前本地同步到的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                //上一次同步的时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            //返回建立了channel连接
            return this.socketChannel != null;
        }

        /**
         * 关闭主站
         */
        private void closeMaster() {
            if (null != this.socketChannel) {//已经与主站建立了channel连接
                try {
                    //获取channel所注册到的SelectionKey
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {//SelectionKey不为null
                        //取消selectionKey
                        sk.cancel();
                    }

                    //关闭channel
                    this.socketChannel.close();

                    //设置channel为null
                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                //设置最后一次向channel写入数据的时间为0
                this.lastWriteTimestamp = 0;
                //设置最后一次分发的位置为0
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * 同步高可用服务commitlog文件数据
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {//服务没有关闭
                try {
                    if (this.connectMaster()) {//如果已经与主站建立了channel连接
                        if (this.isTimeToReportOffset()) {//最后一个本地commitlog文件系统到现在的时间超过5秒 需要将本地的偏移量告诉主站的高可用服务

                            //将从站commitlog的最大偏移量告诉主站的高可用服务
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {//写入失败  主站可能坏了
                                //关闭主站
                                this.closeMaster();
                            }
                        }

                        //等待1秒 判断channel的io事件
                        this.selector.select(1000);

                        //读取同步的Commitlog文件系统的偏移量
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        //将从站当前同步到的commitlog偏移量告诉给主站
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
