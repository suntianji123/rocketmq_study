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
package org.apache.rocketmq.broker.dledger;

import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

/**
 * 广播站角色选举处理类
 */
public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 执行器 执行节点角色变更处理任务
     */
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("DLegerRoleChangeHandler_"));

    /**
     * 广播站控制器
     */
    private BrokerController brokerController;

    /**
     * 广播站消息存储
     */
    private DefaultMessageStore messageStore;

    /**
     * 选举主要的关联commitlog文件对象
     */
    private DLedgerCommitLog dLedgerCommitLog;

    /**
     * 选举服务器
     */
    private DLedgerServer dLegerServer;

    /**
     * 实例化一个广播站角色选择处理对象
     * @param brokerController 广播站控制器
     * @param messageStore 广播站的消息存储对象
     */
    public DLedgerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        //设置广播站控制器
        this.brokerController = brokerController;
        //设置消息存储
        this.messageStore = messageStore;
        //设置角色选举主要关联的commitlog文件对象
        this.dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        //设置选举服务器
        this.dLegerServer = dLedgerCommitLog.getdLedgerServer();
    }

    /**
     * 处理节点角色变更
     * @param term 节点当前状态机轮次
     * @param role 节点角色
     */
    @Override public void handle(long term, MemberState.Role role) {
        Runnable runnable = new Runnable() {
            @Override public void run() {
                //开始时间
                long start = System.currentTimeMillis();
                try {

                    //处理成功
                    boolean succ = true;
                    log.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
                    switch (role) {
                        case CANDIDATE://节点的角色变更为Candidate
                            if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {//如果广播站的角色不是从站 将广播站的角色改为从站
                                brokerController.changeToSlave(dLedgerCommitLog.getId());
                            }
                            break;
                        case FOLLOWER://节点角色变更Follower
                            brokerController.changeToSlave(dLedgerCommitLog.getId());
                            break;
                        case LEADER://节点当前的角色为leader

                            //等待节点commitlog 将待有写入到消费队列consumequeue、indexFile的消息写完
                            while (true) {
                                if (!dLegerServer.getMemberState().isLeader()) {//如果状态机的角色不是leader
                                    //设置处理失败
                                    succ = false;
                                    break;
                                }
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == -1) {//节点最后一条消息的index值为-1
                                    break;
                                }
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == dLegerServer.getdLedgerStore().getCommittedIndex()
                                    && messageStore.dispatchBehindBytes() == 0) {//没有需要写入到消费队列字节数 所有待分发字节已经分发完成
                                    break;
                                }
                                Thread.sleep(100);
                            }
                            if (succ) {//如果处理成功

                                //重新设置消费队列的最小偏移量为commitlog文件系统的最小偏移量
                                //设置消费队列的消费偏移量为消费度列的最大偏移量
                                messageStore.recoverTopicQueueTable();

                                //将广播站的角色改为同步主站
                                brokerController.changeToMaster(BrokerRole.SYNC_MASTER);
                            }
                            break;
                        default:
                            break;
                    }
                    log.info("Finish handling broker role change succ={} term={} role={} currStoreRole={} cost={}", succ, term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start));
                } catch (Throwable t) {
                    log.info("[MONITOR]Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start), t);
                }
            }
        };

        //向执行器添加一个处理节点角色变更的任务
        executorService.submit(runnable);
    }

    @Override public void startup() {

    }

    @Override public void shutdown() {
        executorService.shutdown();
    }
}
