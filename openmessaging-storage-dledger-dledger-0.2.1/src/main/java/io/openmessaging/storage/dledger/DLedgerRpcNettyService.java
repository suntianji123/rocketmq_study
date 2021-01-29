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
 * See the License for the specific language dgoverning permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerRequestCode;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.RequestOrResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 远程rpc服务 包括server / client
 */
public class DLedgerRpcNettyService extends DLedgerRpcService {

    private static Logger logger = LoggerFactory.getLogger(DLedgerRpcNettyService.class);

    /**
     * netty server服务端
     */
    private NettyRemotingServer remotingServer;

    /**
     * netty client客户端
     */
    private NettyRemotingClient remotingClient;

    /**
     * 节点状态机
     */
    private MemberState memberState;

    /**
     * 节点服务器
     */
    private DLedgerServer dLedgerServer;

    private ExecutorService futureExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "FutureExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 拉票执行器 如果之前的线程可用 用之前的线程 否则线程将会回收掉
     */
    private ExecutorService voteInvokeExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "voteInvokeExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 发送心跳请求的任务执行器
     */
    private ExecutorService heartBeatInvokeExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private AtomicInteger threadIndex = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "heartBeatInvokeExecutor_" + this.threadIndex.incrementAndGet());
        }
    });

    /**
     * 实例化一个远程rpc服务
     * @param dLedgerServer 节点服务器
     */
    public DLedgerRpcNettyService(DLedgerServer dLedgerServer) {
        //设置节点服务器
        this.dLedgerServer = dLedgerServer;

        //设置节点状态机
        this.memberState = dLedgerServer.getMemberState();

        //接收到其他节点请求的处理器
        NettyRequestProcessor protocolProcessor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return DLedgerRpcNettyService.this.processRequest(ctx, request);
            }

            //是否拒绝请求 返回false
            @Override public boolean rejectRequest() {
                return false;
            }
        };

        //实例化一个netty server配置
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        //设置接收远程连接的监听端口
        nettyServerConfig.setListenPort(Integer.valueOf(memberState.getSelfAddr().split(":")[1]));
        //实例化一个Netty server
        this.remotingServer = new NettyRemotingServer(nettyServerConfig, null);
        //注册请求码对应的处理器
        this.remotingServer.registerProcessor(DLedgerRequestCode.METADATA.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.APPEND.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.GET.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.PULL.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.PUSH.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.VOTE.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.HEART_BEAT.getCode(), protocolProcessor, null);
        this.remotingServer.registerProcessor(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), protocolProcessor, null);

        //设置netty client
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);

    }

    /**
     * 根据请求获取对端节点地址
     * @param request 请求对象
     * @return
     */
    private String getPeerAddr(RequestOrResponse request) {
        //support different groups in the near future
        return memberState.getPeerAddr(request.getRemoteId());
    }

    /**
     * 给对端节点发送心跳请求
     * @param request 心跳请求对象
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception {
        //实例化一个异步操作对象
        CompletableFuture<HeartBeatResponse> future = new CompletableFuture<>();

        heartBeatInvokeExecutor.execute(() -> {
            try {
                //创建一个心跳的请求
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.HEART_BEAT.getCode(), null);
                //设置远程命令的请求体为请求转为的字节数组
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                //异步发送异步请求 请求超时时间为3秒
                remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                    //获取响应
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {//对端节点有响应
                        //获取心跳的响应
                        HeartBeatResponse response = JSON.parseObject(responseCommand.getBody(), HeartBeatResponse.class);
                        //将心跳响应设置到异步操作结果
                        future.complete(response);
                    } else {
                        logger.error("HeartBeat request time out, {}", request.baseInfo());
                        future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
                    }
                });
            } catch (Throwable t) {
                logger.error("Send heartBeat request failed, {}", request.baseInfo(), t);
                future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
            }
        });
        return future;
    }

    /**
     * 发起拉票请求
     * @param request 拉票情趣
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception {
        //实例化一个异步操作对象
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        //向投票执行器添加一个任务
        voteInvokeExecutor.execute(() -> {
            try {
                //创建一个远程命令行请求 请求码为拉票
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.VOTE.getCode(), null);
                //设置请求体 将请求序列化为字节数组
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                //向对端节点发起异步的拉票请求 超时时间为3秒 对端响应后添加回调
                remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                    //获取响应的命令行对象
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {//对端有响应
                        //将响应体反序列化拉票响应
                        VoteResponse response = JSON.parseObject(responseCommand.getBody(), VoteResponse.class);
                        //将响应设置到异步操作的结果
                        future.complete(response);
                    } else {//对端地址不存在
                        logger.error("Vote request time out, {}", request.baseInfo());
                        future.complete(new VoteResponse());
                    }
                });
            } catch (Throwable t) {
                logger.error("Send vote request failed, {}", request.baseInfo(), t);
                future.complete(new VoteResponse());
            }
        });
        return future;
    }

    @Override public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        GetEntriesResponse entriesResponse = new GetEntriesResponse();
        entriesResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(entriesResponse);
    }

    @Override public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        CompletableFuture<AppendEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.APPEND.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    AppendEntryResponse response = JSON.parseObject(responseCommand.getBody(), AppendEntryResponse.class);
                    future.complete(response);
                } else {
                    AppendEntryResponse response = new AppendEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send append request failed, {}", request.baseInfo(), t);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    @Override public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        MetadataResponse metadataResponse = new MetadataResponse();
        metadataResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(metadataResponse);
    }

    @Override public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PULL.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(getPeerAddr(request), wrapperRequest, 3000);
        PullEntriesResponse response = JSON.parseObject(wrapperResponse.getBody(), PullEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PUSH.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    PushEntryResponse response = JSON.parseObject(responseCommand.getBody(), PushEntryResponse.class);
                    future.complete(response);
                } else {
                    PushEntryResponse response = new PushEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send push request failed, {}", request.baseInfo(), t);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> leadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        CompletableFuture<LeadershipTransferResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();
                if (responseCommand != null) {
                    LeadershipTransferResponse response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), LeadershipTransferResponse.class);
                    future.complete(response);
                } else {
                    LeadershipTransferResponse response = new LeadershipTransferResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                    future.complete(response);
                }
            });
        } catch (Throwable t) {
            logger.error("Send leadershipTransfer request failed, {}", request.baseInfo(), t);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    private void writeResponse(RequestOrResponse storeResp, Throwable t, RemotingCommand request,
        ChannelHandlerContext ctx) {
        RemotingCommand response = null;
        try {
            if (t != null) {
                //the t should not be null, using error code instead
                throw t;
            } else {
                response = handleResponse(storeResp, request);
                response.markResponseType();
                ctx.writeAndFlush(response);
            }
        } catch (Throwable e) {
            logger.error("Process request over, but fire response failed, request:[{}] response:[{}]", request, response, e);
        }
    }

    /**
     * 处理对端节点发起的请求
     * @param ctx 与对端节点建立的channel连接
     * @param request 对端节点发起的请求
     * @return
     * @throws Exception
     */
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {

        //获取请求码
        DLedgerRequestCode requestCode = DLedgerRequestCode.valueOf(request.getCode());
        switch (requestCode) {
            case METADATA: {
                MetadataRequest metadataRequest = JSON.parseObject(request.getBody(), MetadataRequest.class);
                CompletableFuture<MetadataResponse> future = handleMetadata(metadataRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case APPEND: {
                AppendEntryRequest appendEntryRequest = JSON.parseObject(request.getBody(), AppendEntryRequest.class);
                CompletableFuture<AppendEntryResponse> future = handleAppend(appendEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case GET: {
                GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                CompletableFuture<GetEntriesResponse> future = handleGet(getEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PULL: {
                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
                CompletableFuture<PullEntriesResponse> future = handlePull(pullEntriesRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case PUSH: {
                PushEntryRequest pushEntryRequest = JSON.parseObject(request.getBody(), PushEntryRequest.class);
                CompletableFuture<PushEntryResponse> future = handlePush(pushEntryRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case VOTE: {//拉票
                //反序列化拉票请求
                VoteRequest voteRequest = JSON.parseObject(request.getBody(), VoteRequest.class);
                //处理拉票请求 返回响应的异步操作
                CompletableFuture<VoteResponse> future = handleVote(voteRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case HEART_BEAT: {
                //解码消息体 获取心跳请求
                HeartBeatRequest heartBeatRequest = JSON.parseObject(request.getBody(), HeartBeatRequest.class);
                CompletableFuture<HeartBeatResponse> future = handleHeartBeat(heartBeatRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case LEADERSHIP_TRANSFER: {
                long start = System.currentTimeMillis();
                LeadershipTransferRequest leadershipTransferRequest = JSON.parseObject(request.getBody(), LeadershipTransferRequest.class);
                CompletableFuture<LeadershipTransferResponse> future = handleLeadershipTransfer(leadershipTransferRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                    logger.info("LEADERSHIP_TRANSFER FINISHED. Request={}, response={}, cost={}ms",
                        request, x, DLedgerUtils.elapsed(start));
                }, futureExecutor);
                break;
            }
            default:
                logger.error("Unknown request code {} from {}", request.getCode(), request);
                break;
        }
        return null;
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        return dLedgerServer.handleLeadershipTransfer(leadershipTransferRequest);
    }

    /**
     * 处理心跳
     * @param request 心跳请求
     * @return
     * @throws Exception
     */
    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return dLedgerServer.handleHeartBeat(request);
    }

    /**
     * 处理对端节点发起的拉票请求
     * @param request 拉票请求
     * @return
     * @throws Exception
     */
    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        VoteResponse response = dLedgerServer.handleVote(request).get();
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return dLedgerServer.handleAppend(request);
    }

    @Override public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return dLedgerServer.handleGet(request);
    }

    @Override public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return dLedgerServer.handleMetadata(request);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return dLedgerServer.handlePull(request);
    }

    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return dLedgerServer.handlePush(request);
    }

    public RemotingCommand handleResponse(RequestOrResponse response, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(DLedgerResponseCode.SUCCESS.getCode(), null);
        remotingCommand.setBody(JSON.toJSONBytes(response));
        remotingCommand.setOpaque(request.getOpaque());
        return remotingCommand;
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.remotingClient.start();
    }

    @Override
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }

    public DLedgerServer getdLedgerServer() {
        return dLedgerServer;
    }

    public void setdLedgerServer(DLedgerServer dLedgerServer) {
        this.dLedgerServer = dLedgerServer;
    }
}
