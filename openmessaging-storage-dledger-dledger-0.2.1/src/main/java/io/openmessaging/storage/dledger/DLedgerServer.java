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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.DLedgerProtocolHander;
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
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerServer implements DLedgerProtocolHander {

    private static Logger logger = LoggerFactory.getLogger(DLedgerServer.class);

    /**
     * 节点状态机
     */
    private MemberState memberState;

    /**
     * 节点配置
     */
    private DLedgerConfig dLedgerConfig;

    private DLedgerStore dLedgerStore;

    /**
     * 远程rpc服务
     */
    private DLedgerRpcService dLedgerRpcService;
    private DLedgerEntryPusher dLedgerEntryPusher;

    /**
     * leader选取实现器
     */
    private DLedgerLeaderElector dLedgerLeaderElector;

    private ScheduledExecutorService executorService;

    /**
     * 实例化一个服务器节点
     * @param dLedgerConfig 节点配置
     */
    public DLedgerServer(DLedgerConfig dLedgerConfig) {
        //设置节点配置
        this.dLedgerConfig = dLedgerConfig;
        //设置状态机
        this.memberState = new MemberState(dLedgerConfig);
        //设置文件存储
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);

        //设置rpc服务
        dLedgerRpcService = new DLedgerRpcNettyService(this);
        //数据推送器
        dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);

        //实例化一个leader选举实现器
        dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);

        //延时任务执行器
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            //仙剑线程
            Thread t = new Thread(r);
            //设置为守护线程
            t.setDaemon(true);
            //设置工作线程名字
            t.setName("DLedgerServer-ScheduledExecutor");
            return t;
        });
    }


    /**
     * 启动节点服务器
     */
    public void startup() {
        //启动消息存储对象
        this.dLedgerStore.startup();
        //启动rpc server / rpc client
        this.dLedgerRpcService.startup();

        //启动消息推送器
        this.dLedgerEntryPusher.startup();

        //启动选举实现器
        this.dLedgerLeaderElector.startup();
        executorService.scheduleAtFixedRate(this::checkPreferredLeader, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.dLedgerLeaderElector.shutdown();
        this.dLedgerEntryPusher.shutdown();
        this.dLedgerRpcService.shutdown();
        this.dLedgerStore.shutdown();
        executorService.shutdown();
    }

    /**
     * 创建一个信息存储对象
     * @param storeType 消息存储类型：文件、内存
     * @param config 节点配置
     * @param memberState 节点状态机
     * @return
     */
    private DLedgerStore createDLedgerStore(String storeType, DLedgerConfig config, MemberState memberState) {
        if (storeType.equals(DLedgerConfig.MEMORY)) {//消息存储类型为内存
            return new DLedgerMemoryStore(config, memberState);
        } else {//消息存储类型为文件
            return new DLedgerMmapFileStore(config, memberState);
        }
    }

    public MemberState getMemberState() {
        return memberState;
    }

    /**
     * 处理心跳
     * @param request 心跳请求
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {
            //当前节点的id必须与心跳请求的对端节点相同
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            //当前节点的集群名必须与请求的对端节点的集群名相同
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleHeartBeat(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * 处理拉票请求
     * @param request 对端节点发起的拉票请求对象
     * @return
     * @throws Exception
     */
    @Override public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        try {
            //请求的目标节点id必须与当前节点id相等
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            //请求的目标集群名必须与当前节点所处的集群一致
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleVote(request, false);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }


    /**
     * 处理向DledgerCommitlog中添加消息 向本地的commitlog中添加消息 并且添加一个异步操作同步给远程节点
     * @param request 请求对象
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws IOException {
        try {

            //当前节点的id必须是请求的id
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            //当前节点的集群名必须是请求的集群名
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            //当前节点的角色必须为leader
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            //主节点不能有委托人
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING);

            //获取当前状态机的轮次
            long currTerm = memberState.currTerm();
            if (dLedgerEntryPusher.isPendingFull(currTerm)) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setGroup(memberState.getGroup());
                appendEntryResponse.setCode(DLedgerResponseCode.LEADER_PENDING_FULL.getCode());
                appendEntryResponse.setTerm(currTerm);
                appendEntryResponse.setLeaderId(memberState.getSelfId());
                return AppendFuture.newCompletedFuture(-1, appendEntryResponse);
            } else {
                if (request instanceof BatchAppendEntryRequest) {
                    BatchAppendEntryRequest batchRequest = (BatchAppendEntryRequest) request;
                    if (batchRequest.getBatchMsgs() != null && batchRequest.getBatchMsgs().size() != 0) {
                        // record positions to return;
                        long[] positions = new long[batchRequest.getBatchMsgs().size()];
                        DLedgerEntry resEntry = null;
                        // split bodys to append
                        int index = 0;
                        Iterator<byte[]> iterator = batchRequest.getBatchMsgs().iterator();
                        while (iterator.hasNext()) {
                            DLedgerEntry dLedgerEntry = new DLedgerEntry();
                            dLedgerEntry.setBody(iterator.next());
                            resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                            positions[index++] = resEntry.getPos();
                        }
                        // only wait last entry ack is ok
                        BatchAppendFuture<AppendEntryResponse> batchAppendFuture =
                                (BatchAppendFuture<AppendEntryResponse>) dLedgerEntryPusher.waitAck(resEntry, true);
                        batchAppendFuture.setPositions(positions);
                        return batchAppendFuture;
                    }
                    throw new DLedgerException(DLedgerResponseCode.REQUEST_WITH_EMPTY_BODYS, "BatchAppendEntryRequest" +
                            " with empty bodys");
                } else {
                    //实例化一个存放于dlegerCommitlog中的消息实体
                    DLedgerEntry dLedgerEntry = new DLedgerEntry();
                    //设置消息体
                    dLedgerEntry.setBody(request.getBody());

                    //向leader节点date index mappedFileList中添加消息 返回添加到leader节点的消息实体
                    DLedgerEntry resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                    return dLedgerEntryPusher.waitAck(resEntry, false);
                }
            }
        } catch (DLedgerException e) {
            logger.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            DLedgerEntry entry = dLedgerStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleGet] failed", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setLeaderId(memberState.getLeaderId());
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleMetadata] failed", memberState.getSelfId(), e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) {
        return null;
    }

    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerEntryPusher.handlePush(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            if (memberState.getSelfId().equals(request.getTransferId())) {
                //It's the leader received the transfer command.
                PreConditions.check(memberState.isPeerMember(request.getTransfereeId()), DLedgerResponseCode.UNKNOWN_MEMBER, "transferee=%s is not a peer member", request.getTransfereeId());
                PreConditions.check(memberState.currTerm() == request.getTerm(), DLedgerResponseCode.INCONSISTENT_TERM, "currTerm(%s) != request.term(%s)", memberState.currTerm(), request.getTerm());
                PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER, "selfId=%s is not leader=%s", memberState.getSelfId(), memberState.getLeaderId());

                // check fall transferee not fall behind much.
                long transfereeFallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(request.getTerm(), request.getTransfereeId());
                PreConditions.check(transfereeFallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex(),
                    DLedgerResponseCode.FALL_BEHIND_TOO_MUCH, "transferee fall behind too much, diff=%s", transfereeFallBehind);
                return dLedgerLeaderElector.handleLeadershipTransfer(request);
            } else if (memberState.getSelfId().equals(request.getTransfereeId())) {
                // It's the transferee received the take leadership command.
                PreConditions.check(request.getTransferId().equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, "transfer=%s is not leader", request.getTransferId());

                return dLedgerLeaderElector.handleTakeLeadership(request);
            } else {
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
            }
        } catch (DLedgerException e) {
            logger.error("[{}][handleLeadershipTransfer] failed", memberState.getSelfId(), e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    private void checkPreferredLeader() {
        if (!memberState.isLeader()) {
            return;
        }
        String preferredLeaderId = dLedgerConfig.getPreferredLeaderId();
        if (preferredLeaderId == null || preferredLeaderId.equals(dLedgerConfig.getSelfId())) {
            return;
        }

        if (!memberState.isPeerMember(preferredLeaderId)) {
            logger.warn("preferredLeaderId = {} is not a peer member", preferredLeaderId);
            return;
        }

        if (memberState.getTransferee() != null) {
            return;
        }

        if (!memberState.getPeersLiveTable().containsKey(preferredLeaderId) ||
            memberState.getPeersLiveTable().get(preferredLeaderId) == Boolean.FALSE) {
            logger.warn("preferredLeaderId = {} is not online", preferredLeaderId);
            return;
        }

        long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), preferredLeaderId);
        logger.info("transferee fall behind index : {}", fallBehind);
        if (fallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setTerm(memberState.currTerm());
            request.setTransfereeId(dLedgerConfig.getPreferredLeaderId());

            try {
                long startTransferTime = System.currentTimeMillis();
                LeadershipTransferResponse response = dLedgerLeaderElector.handleLeadershipTransfer(request).get();
                logger.info("transfer finished. request={},response={},cost={}ms", request, response, DLedgerUtils.elapsed(startTransferTime));
            } catch (Throwable t) {
                logger.error("[checkPreferredLeader] error, request={}", request, t);
            }
        }
    }

    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }

    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public DLedgerLeaderElector getdLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    public DLedgerConfig getdLedgerConfig() {
        return dLedgerConfig;
    }
}
