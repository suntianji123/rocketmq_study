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

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息推送器
 */
public class DLedgerEntryPusher {

    private static Logger logger = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    /**
     * 节点配置
     */
    private DLedgerConfig dLedgerConfig;

    /**
     * 消息存储
     */
    private DLedgerStore dLedgerStore;

    /**
     * 节点状态机
     */
    private final MemberState memberState;

    /**
     * rpc服务
     */
    private DLedgerRpcService dLedgerRpcService;

    /**
     * 轮次 - 节点id - 存放消息的index值
     */
    private Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    /**
     * 轮次-向commitlog中添加消息的index -异步操作对象
     */
    private Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    /**
     * 实体处理器
     */
    private EntryHandler entryHandler;

    private QuorumAckChecker quorumAckChecker;

    /**
     * 对端节点id|分发器实体 map
     */
    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    /**
     * 实例化数据推送者
     * @param dLedgerConfig 节点配置
     * @param memberState 节点状态机
     * @param dLedgerStore 节点消息存储对象
     * @param dLedgerRpcService 节点rpc服务
     */
    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        //设置节点配置
        this.dLedgerConfig = dLedgerConfig;
        //设置节点状态机
        this.memberState = memberState;
        //设置节点消息存储
        this.dLedgerStore = dLedgerStore;
        //设置rpc服务
        this.dLedgerRpcService = dLedgerRpcService;

        //遍历状态机中所有节点
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {//过滤掉自身节点
                //对端节点id | 分发器实体
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }

        //设置实体处理器
        this.entryHandler = new EntryHandler(logger);
        this.quorumAckChecker = new QuorumAckChecker(logger);
    }

    /**
     * 启动消息推送器
     */
    public void startup() {
        //启动实体处理器
        entryHandler.start();
        quorumAckChecker.start();

        //启动为每个节点建立的实体分发器
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    /**
     * 处理leader节点推送消息的请求
     * @param request 推送消息的请求
     * @return
     * @throws Exception
     */
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    /**
     * 更新某个节点的消息存放到的index值
     * @param term 轮次
     * @param peerId 节点id
     * @param index 已经存放到的消息的index值
     */
    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    public boolean isPendingFull(long currTerm) {
        checkTermForPendingMap(currTerm, "isPendingFull");
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    /**
     * 向从站节点发送写入消息实体的请求 等待响应
     * @param entry 消息实体
     * @param isBatchWait 是否为批量消息
     * @return
     */
    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry, boolean isBatchWait) {
        //更新当前轮次下  当前节点消息存放的index值
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        if (memberState.getPeerMap().size() == 1) {//只有一个节点 他自己
            //创建响应
            AppendEntryResponse response = new AppendEntryResponse();
            //设置响应的集群名
            response.setGroup(memberState.getGroup());
            //设置leaderId
            response.setLeaderId(memberState.getSelfId());
            //设置消息的index值
            response.setIndex(entry.getIndex());
            //设置消息的轮次
            response.setTerm(entry.getTerm());
            //设置消息的偏移量
            response.setPos(entry.getPos());
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        } else {//存在其他远程节点

            //检查轮次-index-异步响应对象map
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            //响应的异步操作对象
            AppendFuture<AppendEntryResponse> future;
            if (isBatchWait) {//批量提交
                future = new BatchAppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            } else {//单个消息
                future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            }

            //设置将要被添加的消息在commitlog文件系统的偏移量
            future.setPos(entry.getPos());

            //提交一个添加异步操作请求到列表
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }

            //返回异步操作对象
            return future;
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        /**
         * 主节点同步数据给其他节点
         */
        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {//不是leader节点 直接返回
                    waitForRunning(1);
                    return;
                }

                //获取当前状态机轮次
                long currTerm = memberState.currTerm();

                //检查轮次-index-向其他节点添加消息的异步操作对象map不为nulll
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                //检查轮次-节点id-index值 map不为null
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                if (pendingAppendResponsesByTerm.size() > 1) {//存在向其他节点添加消息的异步操作
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {//处理之前轮次的异步操作
                        if (term == currTerm) {//过滤当前轮次的异步操作
                            continue;
                        }
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {//遍历所有index值的异步操作
                            //实例化一个添加消息的响应
                            AppendEntryResponse response = new AppendEntryResponse();
                            //设置集群组名
                            response.setGroup(memberState.getGroup());
                            //设置index值
                            response.setIndex(futureEntry.getKey());
                            //设置响应码
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            //设置leaderId
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            //设置异步操作结果
                            futureEntry.getValue().complete(response);
                        }

                        //删除添加消息的异步操作
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                if (peerWaterMarksByTerm.size() > 1) {//存在其他节点
                    for (Long term : peerWaterMarksByTerm.keySet()) {//获取从节点的轮次
                        if (term == currTerm) {//过滤当前轮次
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        //删除其他轮次
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                //获取当前轮次的所有节点已经添加的消息的index值
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);

                //从低到高排序其他节点同步到的index值
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                        .stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList());

                //获取中间节点的index值
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);

                //更新提交的commitIndex值
                dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);

                //获取当前轮次所有向其他节点添加消息的异步操作
                ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                boolean needCheck = false;
                int ackNum = 0;
                for (Long i = quorumIndex; i > lastQuorumIndex; i--) {
                    try {
                        CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                        if (future == null) {
                            needCheck = true;
                            break;
                        } else if (!future.isDone()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) future).getPos());
                            future.complete(response);
                        }
                        ackNum++;
                    } catch (Throwable t) {
                        logger.error("Error in ack to index={} term={}", i, currTerm, t);
                    }
                }

                if (ackNum == 0) {
                    for (long i = quorumIndex + 1; i < Integer.MAX_VALUE; i++) {
                        TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                        if (future == null) {
                            break;
                        } else if (future.isTimeOut()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                            response.setTerm(currTerm);
                            response.setIndex(i);
                            response.setLeaderId(memberState.getSelfId());
                            future.complete(response);
                        } else {
                            break;
                        }
                    }
                    waitForRunning(1);
                }

                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                    updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                    for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
                        if (futureEntry.getKey() < quorumIndex) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setTerm(currTerm);
                            response.setIndex(futureEntry.getKey());
                            response.setLeaderId(memberState.getSelfId());
                            response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                            futureEntry.getValue().complete(response);
                            responses.remove(futureEntry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                lastQuorumIndex = quorumIndex;
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }


    /**
     * 每一个对端节点对应的实体分发器
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        /**
         * 分发器类型
         */
        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);

        /**
         * 上一次提交的时间
         */
        private long lastPushCommitTimeMs = -1;

        /**
         * 对端节点id
         */
        private String peerId;

        /**
         *比较index
         */
        private long compareIndex = -1;

        /**
         * 向对端节点同步消息的起始下标
         */
        private long writeIndex = -1;
        private int maxPendingSize = 1000;

        /**
         * 最后一条消息的轮次
         */
        private long term = -1;

        /**
         * leaderId
         */
        private String leaderId = null;

        /**
         * 最后一次检查的时间
         */
        private long lastCheckLeakTimeMs = System.currentTimeMillis();

        /**
         * 向对端同步消息的index | 请求时间 map
         */
        private ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        private ConcurrentMap<Long, Pair<Long, Integer>> batchPendingMap = new ConcurrentHashMap<>();
        private PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();
        private Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        /**
         * 实例化一个分发器实体
         * @param peerId 节点id
         * @param logger 日志对象
         */
        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            //对端节点id
            this.peerId = peerId;
        }

        /**
         * 检查是否可以刷新实体分发器的状态
         * @return
         */
        private boolean checkAndFreshState() {
            if (!memberState.isLeader()) {//当前节点的角色必须为leader
                return false;
            }

            //最后一条消息的轮次必须等于当前节点的状态机的轮次 leaderId必须存在 并且leaderId与状态机的leaderId相等
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {//加锁状态机
                    if (!memberState.isLeader()) {//当前节点不是leader 直接返回
                        return false;
                    }

                    //检查当前节点必须是leaderId
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    //设置实体分发器的轮次
                    term = memberState.currTerm();
                    //设置leaderId
                    leaderId = memberState.getSelfId();
                    //改变分发器的状态为比较
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        /**
         * 构建推送请求
         * @param entry leader节点消息实体
         * @param target 推送类型
         * @return
         */
        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            //实例化一个推送实体的请求对象
            PushEntryRequest request = new PushEntryRequest();

            //设置集群组名
            request.setGroup(memberState.getGroup());
            //设置兑点节点id
            request.setRemoteId(peerId);
            //设置leaderId
            request.setLeaderId(leaderId);
            //轮次
            request.setTerm(term);
            //消息实体
            request.setEntry(entry);
            //推送乐西
            request.setType(target);
            //当前节点的提交位置
            request.setCommitIndex(dLedgerStore.getCommittedIndex());
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        /**
         * 检查同步消息的index 与leader节点最后一条消息的index
         * @param entry
         */
        private void checkQuotaAndWait(DLedgerEntry entry) {
            //需要同步消息的数量小于最大值
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }

            //内存
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;

            //同步消息的大小不超过300M
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        /**
         * 向对端节点同步消息
         * @param index 同步消息的index
         * @throws Exception
         */
        private void doAppendInner(long index) throws Exception {
            //获取同步消息
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }

            //检查等待
            checkQuotaAndWait(entry);
            //创建请求
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);

            //向对端节点添加消息 返回异步操作
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            //向集合中添加一个消息对应请求entry
            pendingMap.put(index, System.currentTimeMillis());
            responseFuture.whenComplete((x, ex) -> {
                try {
                    //不能有异常
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    //虎丘响应码
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            //移除
                            pendingMap.remove(x.getIndex());
                            //更新对端节点已经同步到的index值
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            //唤醒检查
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        /**
         * 获取leader节点同步消息
         * @param index 消息index
         * @return
         */
        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            //消息实体
            DLedgerEntry entry;
            try {
                //获取消息实体
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                    return null;
                }
                throw e;
            }

            //消息实体不为null
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        /**
         * 更新对端节点的commitIndex
         * @throws Exception
         */
        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLedgerRpcService.push(request);
                //设置上一次更新CommitIndex的时间
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        /**
         * 向对端节点添加消息
         * @throws Exception
         */
        private void doCheckAppendResponse() throws Exception {
            //获取对端节点已经同步到的位置
            long peerWaterMark = getPeerWaterMark(term, peerId);
            //获取发送时间
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {//超过1秒没有处理 再次添加一个
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

        /**
         * 向对端节点发送同步消息请求 同步消息
         * @throws Exception
         */
        private void doAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {//检查状态
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {//推送器的状态为append
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {//所有的消息已经同步完
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }

                //请求添加消息的map请求过多 清理掉老的废弃的情趣
                if (pendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }

                //向对端节点添加消息
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }

        private void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(dLedgerStore.getCommittedIndex());
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            batchPendingMap.remove(x.getIndex());
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }

        private void doBatchAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            batchAppendEntryRequest.addEntry(entry);
            if (batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchPushSize()) {
                sendBatchAppendEntryRequest();
            }
        }

        private void doCheckBatchAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Pair pair = batchPendingMap.get(peerWaterMark + 1);
            if (pair != null && System.currentTimeMillis() - (long) pair.getKey() > dLedgerConfig.getMaxPushTimeOutMs()) {
                long firstIndex = peerWaterMark + 1;
                long lastIndex = firstIndex + (int) pair.getValue() - 1;
                logger.warn("[Push-{}]Retry to push entry from {} to {}", peerId, firstIndex, lastIndex);
                batchAppendEntryRequest.clear();
                for (long i = firstIndex; i <= lastIndex; i++) {
                    DLedgerEntry entry = dLedgerStore.get(i);
                    batchAppendEntryRequest.addEntry(entry);
                }
                sendBatchAppendEntryRequest();
            }
        }

        private void doBatchAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (batchAppendEntryRequest.getCount() > 0) {
                        sendBatchAppendEntryRequest();
                    }
                    doCommit();
                    doCheckBatchAppendResponse();
                    break;
                }
                if (batchPendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : batchPendingMap.entrySet()) {
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            batchPendingMap.remove(entry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (batchPendingMap.size() >= maxPendingSize) {
                    doCheckBatchAppendResponse();
                    break;
                }
                doBatchAppendInner(writeIndex);
                writeIndex++;
            }
        }

        /**
         * 让对端节点删除truncateIndex之前的消息
         * @param truncateIndex 删除的下标
         * @throws Exception
         */
        private void doTruncate(long truncateIndex) throws Exception {
            //实体推送器的类型为截短
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            //获取当前leader节点truncateIndex位置处的实体
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            //截取位置处的实体不为null
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            //实例化一个截短请求
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            //推送截短请求  获取响应
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            //截短响应不为nulll
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            //响应码为成功
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            //设置上一次推送提交的时间
            lastPushCommitTimeMs = System.currentTimeMillis();
            //改变推送器的类型为添加消息
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        /**
         * 改变实体分发器的状态
         * @param index 对端节点已经写到的位置
         * @param target 状态类型
         */
        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND://添加消息
                    compareIndex = -1;//设置comareIndex = -1
                    //更新对端节点已经同步到的位置
                    updatePeerWaterMark(term, peerId, index);
                    //启动ackChecker检测 立刻执行
                    quorumAckChecker.wakeup();
                    //同步消息的起始下标
                    writeIndex = index + 1;
                    if (dLedgerConfig.isEnableBatchPush()) {
                        resetBatchAppendEntryRequest();
                    }
                    break;
                case COMPARE://和对端节点compareIndex位置的消息进行比较
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {//由append变为compare类型
                        //设置比较下标为-1
                        compareIndex = -1;
                        if (dLedgerConfig.isEnableBatchPush()) {//允许批量推送
                            batchPendingMap.clear();
                        } else {
                            pendingMap.clear();
                        }
                    }
                    break;
                case TRUNCATE://截短对端节点指定位置之前的消息
                    compareIndex = -1;
                    break;
                default:
                    break;
            }

            //设置目标的类型为比较
            type.set(target);
        }

        /**
         * 执行比较逻辑
         * 比较对端节点的最后一条消息的index 与当前leader节点最后一条消息的index
         * @throws Exception
         */
        private void doCompare() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {//检查推送器的状态
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {//如果不是compare状态 返回
                    break;
                }

                //当前leader没有写入消息
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }

                if (compareIndex == -1) {//当前leader节点写入了消息
                    //设置对端节点的比较index值
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {//对端节点的比较index不在当前节点index的范围内
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    //设置对端节点的比较index为当前节点最后一条消息的index
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }

                //获取比较index位置的消息实体
                DLedgerEntry entry = dLedgerStore.get(compareIndex);

                //实体不能null
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);

                //构建一个比较请求
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);

                //向对端节点推送消息请求
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);

                //等待3秒 获取异步操作结果
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                //响应存在
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                //响应码
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);

                //对端节点将要截断的index的起始值  对端节点将会将index值小于truncateIndex之前的消息删除
                long truncateIndex = -1;

                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {//如果对端节点存在存在比较消息
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex()
                    || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {//对端节点不存在compareIndex位置的比较消息 对端节点的消息index不在当前节点的位置范围内
                    //设置为第一条消息的截断index值
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex > response.getEndIndex()) {
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    compareIndex = response.getEndIndex();
                } else {
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    compareIndex--;
                }
                /*
                 The compared index is smaller than the leader's begin index, truncate the follower.
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /*
                 If get value for truncateIndex, do it right now.
                 */
                if (truncateIndex != -1) {
                    //设置实体推送器的状态为截短
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);

                    //删除对端节点truncateIndex之前的消息
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        @Override
        public void doWork() {
            try {
                if (!checkAndFreshState()) {//检查对端节点对应的实体推送器的状态
                    waitForRunning(1);
                    return;
                }

                if (type.get() == PushEntryRequest.Type.APPEND) {
                    if (dLedgerConfig.isEnableBatchPush()) {
                        doBatchAppend();
                    } else {
                        //向对端节点发送同步消息请求 同步消息
                        doAppend();
                    }
                } else {//对端实体推送器的状态为compare
                    //执行比较逻辑
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                DLedgerUtils.sleep(500);
            }
        }
    }

    /**
     * 实体处理器
     */
    private class EntryHandler extends ShutdownAbleThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        //比较entry的请求与结果异步操作的队列
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        /**
         * 实例化一个实体处理器
         * @param logger 日志对象
         */
        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        /**
         * 处理leader节点推送消息的请求
         * @param request 推送消息的请求
         * @return
         * @throws Exception
         */
        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            //实例化一个超时的异步操作
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    if (dLedgerConfig.isEnableBatchPush()) {
                        PreConditions.check(request.getBatchEntry() != null && request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                        long firstIndex = request.getFirstEntryIndex();
                        writeRequestMap.put(firstIndex, new Pair<>(request, future));
                    } else {
                        PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                        long index = request.getEntry().getIndex();
                        Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                        if (old != null) {
                            logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                            future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                        }
                    }
                    break;
                case COMMIT:
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE://比较
                case TRUNCATE:
                    //leader节点比较的消息实体不能为null
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    //向队列中添加一个比较的异步
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            return future;
        }

        /**
         * 构建推送实体的响应
         * @param request 推送消息的请求
         * @param code 响应码
         * @return
         */
        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            //实例化一个推送实体的响应
            PushEntryResponse response = new PushEntryResponse();
            //设置集群组名
            response.setGroup(request.getGroup());
            //设置响应码
            response.setCode(code);
            //设置轮次
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                //设置消息index
                response.setIndex(request.getEntry().getIndex());
            }
            //设置当前节点第一条消息的index
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            //设置当前节点最后一条消息的index
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        private PushEntryResponse buildBatchAppendResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            response.setIndex(request.getLastEntryIndex());
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        /**
         * 对端节点处理leader节点推送的同步消息请求
         * @param writeIndex leader同步消息的index
         * @param request 请求
         * @param future 异步操作
         */
        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                //向当前节点的消息 mappedfile list和索引 mappedfile list添加消息
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                //完成
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                //更新committedIndex
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * 处理leader节点推送的比较实体请求
         * @param compareIndex 比较消息实体的index值
         * @param request 比较消息实体请求
         * @param future 比较请求的异步操作
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                //比较index必须与请求的消息的index相等
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                //比较的类型必须为compare
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                //获取本地的比较消息
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                //抛出异常 本地不存在leader节点对应的比较消息实体
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                //完成异步操作
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * 更新上一次commitIndex的时间
         * @param committedIndex commitIndex值
         * @param request 请求
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                //更新CommitIndex
                dLedgerStore.updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * 截断当期节点的消息 保持与leader节点的index范围同步
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                //截断位置与请求消息实体的index值相等
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                //请求消息的类型为截短
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);

                //截短对端节点truncateIndex之后的消息
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                //设置异步操作完成
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                //更新commitIndex
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoBatchAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getBatchEntry()) {
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildBatchAppendResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                dLedgerStore.updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoBatchAppend]", t);
            }

        }

        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long index = pair.getKey().getEntry().getIndex();
                //Fall behind
                if (index <= endIndex) {
                    try {
                        DLedgerEntry local = dLedgerStore.get(index);
                        PreConditions.check(pair.getKey().getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an entry index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", index, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(index);
                    continue;
                }
                //Just OK
                if (index == endIndex + 1) {
                    //The next entry is coming, just return
                    return;
                }
                //Fast forward
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (index < minFastForwardIndex) {
                    minFastForwardIndex = index;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        private void checkBatchAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                //Fall behind
                if (lastEntryIndex <= endIndex) {
                    try {
                        for (DLedgerEntry dLedgerEntry : pair.getKey().getBatchEntry()) {
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (firstEntryIndex < minFastForwardIndex) {
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildBatchAppendResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }
        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }
            if (dLedgerConfig.isEnableBatchPush()) {
                checkBatchAppendFuture(endIndex);
            } else {
                checkAppendFuture(endIndex);
            }
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {//必须是follower
                    waitForRunning(1);
                    return;
                }

                if (compareOrTruncateRequests.peek() != null) {//leader节点推送了比较实体请求等待处理
                    //获取比较实体请求与异步操作
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {//判断leader节点推送的类型
                        case TRUNCATE://截短对端节点
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE://比较
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    if (dLedgerConfig.isEnableBatchPush()) {
                        handleDoBatchAppend(nextIndex, request, pair.getValue());
                    } else {
                        handleDoAppend(nextIndex, request, pair.getValue());
                    }
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
