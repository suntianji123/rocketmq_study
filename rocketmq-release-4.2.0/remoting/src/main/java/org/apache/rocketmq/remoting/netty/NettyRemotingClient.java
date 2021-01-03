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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * netty client类
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 加锁事件
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    /**
     * netty client配置对象
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * netty client启动配置对象
     */
    private final Bootstrap bootstrap = new Bootstrap();

    /**
     * 轮训通道状态的执行器组
     */
    private final EventLoopGroup eventLoopGroupWorker;

    /**
     * 范文channelTable的锁
     */
    private final Lock lockChannelTables = new ReentrantLock();
    /**
     * netty client已经与远程netty server建立过的channel列表
     */
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    /**
     * 定时器对象 守护线程
     */
    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    /**
     * 远程注册中心地址列表引用对象
     */
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();

    /**
     * 已经选择过的注册中心地址引用
     */
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();

    /**
     * 已经从namesrvAddrList 获取过地址的次数
     */
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());

    /**
     * 连接注册中心服务器锁对象
     */
    private final Lock lockNamesrvChannel = new ReentrantLock();

    /**
     * 公用的处理器 可以用于处理netty client读取netty server响应后的http事件处理
     */
    private final ExecutorService publicExecutor;

    /**
     * 响应的异步操作对象的执行器
     */
    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private RPCHook rpcHook;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    /**
     * 实例化一个netty client配置
     * @param nettyClientConfig netty client配置
     * @param channelEventListener 通道事件回调处理器
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        //设置netty配置对象
        this.nettyClientConfig = nettyClientConfig;
        //channel 事件回调处理
        this.channelEventListener = channelEventListener;

        //公共执行线程数
        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {//公共执行器线程数 没有设置 为4个
            publicThreadNums = 4;
        }

        //实例化一个固定线程数的线程池的执行器
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            //线程池中实例化线程的名字的后缀
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        //轮训通道状态的执行组 只有一个执行器
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            //执行器线程池中线程的名字后缀
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }

    /**
     * 启动远程客户端
     */
    @Override
    public void start() {
        //默认的io事件执行器组 默认4个单线程池的执行器
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyClientConfig.getClientWorkerThreads(),
            new ThreadFactory() {

                //单线程池执行器工作线程名后缀
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        //netty client启动配置
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    //当channel底层的selectionkey注册到eventloop的selector上之后 执行
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            log.info("Prepend SSL handler");
                        } else {
                            log.warn("Connections are insecure as SSLContext is null!");
                        }
                    }

                    //向pipeline添加handler处理链
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        //启动定时器
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    //扫描响应列表
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        //通道事件的监听器不为空
        if (this.channelEventListener != null) {
            //启动netty事件执行器
            this.nettyEventExecutor.start();
        }
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            return;

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    /**
     * 设置netty client的rpchook
     * @param rpcHook
     */
    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    /**
     * 关闭channel
     * @param channel 将要被关闭的channel
     */
    public void closeChannel(final Channel channel) {
        if (null == channel)//将要被关闭的channel不能为空
            return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //是否需要将channel从列表中移除
                    boolean removeItemFromTable = true;
                    //channel包装对象
                    ChannelWrapper prevCW = null;
                    //远程地址
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {//遍历列表
                        String key = entry.getKey();//获取远程地址
                        ChannelWrapper prev = entry.getValue();//获取channel包装对象
                        if (prev.getChannel() != null) {//包装对象的channel不为空
                            if (prev.getChannel() == channel) {//channel相等
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {//需要将channel从table列表中删除
                        this.channelTables.remove(addrRemote);//将channel从table列表中删除
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        //关闭channel
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    //释放访问channelTable的锁
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    /**
     * 更新netty client的远程地址列表
     * @param addrs
     */
    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        //获取以前的地址引用
        List<String> old = this.namesrvAddrList.get();

        //是否更新过列表
        boolean update = false;

        //将要添加的地址部位空
        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            //若谷是更新
            if (update) {
                //乱序地址列表
                Collections.shuffle(addrs);
                log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);

                //设置netty client的注册中心的地址列表
                this.namesrvAddrList.set(addrs);
            }
        }
    }

    /**
     * 执行远程命令 发送请求
     * @param addr 远程netty server 地址
     * @param request 远程命令对象
     * @param timeoutMillis 执行命令超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        //获取或者创建一个与指定netty server的channel连接
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {//channel正常
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                //向channel中写入request 返回响应的远程命令
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                }
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            //关闭channel
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     *获取或者创建一个与远程netty server的连接channel
     * @param addr 远程netty server地址
     * @return
     * @throws InterruptedException
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (null == addr)//如果地址为空
            return getAndCreateNameserverChannel();

        //地址不为空
        //从channel列表中获取channel
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {//连接正常 返回channel
            return cw.getChannel();
        }

        //创建与指定netty server地址的channel连接
        return this.createChannel(addr);
    }

    /**
     * 获取或者创建一个与注册中心的连接channel
     * @return
     * @throws InterruptedException
     */
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //获取之前已经选择过得注册中心地址
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {//之前有选择过注册中心地址
            //从通道列表中获取之前选择地址的通道
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {//通道连接正常
                //返回通道
                return cw.getChannel();
            }
        }

        //之前没有选择过注册中心地址 从注册中心地址列表中选择一个注册中心地址
        final List<String> addrList = this.namesrvAddrList.get();
        //访问注册中心地址枷锁
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                //获取之前选择过的注册中心地址
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {//之前有选择 过注册中心地址
                    //从channel列表中获取已经与对应地址建立的channel连接对象
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {//连接正常
                        //获取通道
                        return cw.getChannel();
                    }
                }

                //如果之前没有选择过channel
                if (addrList != null && !addrList.isEmpty()) {//从注册中心地址列表中选择一个地址
                    for (int i = 0; i < addrList.size(); i++) {
                        //自增地址下标
                        int index = this.namesrvIndex.incrementAndGet();
                        //index可能为负数 需要取绝对值
                        index = Math.abs(index);
                        //获取namesrvAddrList对应的下标
                        index = index % addrList.size();
                        //获取一个地址
                        String newAddr = addrList.get(index);
                        //设置已经选择的地址
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null)
                            return channelNew;
                    }
                }
            } catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建一个与指定netty server地址的channel连接
     * @param addr 远程netty server地址
     * @return
     * @throws InterruptedException
     */
    private Channel createChannel(final String addr) throws InterruptedException {
        //从channel列表获取这个地址对应的channel
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {//channel连接正常
            cw.getChannel().close();//关闭这个channel
            channelTables.remove(addr);//从channel列表删除这个channel
        }

        //访问channeltable枷锁
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                //是否创建了与netty server的连接
                boolean createNewConnection;
                //从channel列表中获取这个地址对应的channel
                cw = this.channelTables.get(addr);
                if (cw != null) {//如果已经创建了channel

                    if (cw.isOK()) {//channel连接正常
                        cw.getChannel().close();//创建这个channel
                        this.channelTables.remove(addr);//从channel列表中删除这个channel
                        createNewConnection = true;//需要创建一个与远程netty server的连接channel
                    } else if (!cw.getChannelFuture().isDone()) {//如果之前有创建于远程netty server的连接 但是连接还没完成
                        createNewConnection = false;//不需要创建连接
                    } else {
                        this.channelTables.remove(addr);//从channel列表删除这个地址对应的channel
                        //需要创建一个新的连接
                        createNewConnection = true;
                    }
                } else {
                    //需要创建新的连接
                    createNewConnection = true;
                }

                //之前创建过channel并且连接正常 删除这个channel 重新创建一个channel
                //之前没有创建过channel 连接没有完成 则不需要创建新的连接
                if (createNewConnection) {
                    //连接远程netty server 返回连接异步操作对象
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    //包装连接异步操作对象
                    cw = new ChannelWrapper(channelFuture);
                    //将channel放入channel列表
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                //释放锁
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        //创建channel成功
        if (cw != null) {
            //获取异步操作对象
            ChannelFuture channelFuture = cw.getChannelFuture();
            //阻塞当前线程 等待异步操作完成 最多3秒
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {//创建与远程netty server的channel正常
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    //返回channel
                    return cw.getChannel();
                } else {
                    //连接事变
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                //连接远程netty server失败
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
            }
        }

        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 注册netty client的http事件处理器
     * @param requestCode 请求吗
     * @param processor 处理器对象
     * @param executor 执行具体业务逻辑的执行器对象
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        //执行器
        ExecutorService executorThis = executor;
        if (null == executor) {//没有指定具体的执行器 则使用公共的逻辑执行器
            executorThis = this.publicExecutor;
        }

        //将处理器对象 和执行器包装为一个Pair对象
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        //将请求码对象的处理器和执行器包装的Pair对象添加处理器列表
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        /**
         * 连接远程服务器
         * @param ctx               handler所在的channelhandlerContext对象
         * @param remoteAddress     远程服务器地址
         * @param localAddress      本地服务器地址
         * @param promise           连接的异步操作对象
         * @throws Exception
         */
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            //获取本地地址
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            //获取远程地址
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            //交给其他handler来处理connect 连接完成后
            super.connect(ctx, remoteAddress, localAddress, promise);

            //其他handler连接完成之后 向channel事件注册一个连接事件
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 用户触发事件
         * @param ctx ChannelHandlerContext对象
         * @param evt IdleStateEvent
         * @throws Exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {//IdleStateEvent
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    //解析远程服务器地址
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    //关闭channel
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {//channel事件回调对象不为空
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                        //实例化一个Netty事件 将netty事件添加到事件列表
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
