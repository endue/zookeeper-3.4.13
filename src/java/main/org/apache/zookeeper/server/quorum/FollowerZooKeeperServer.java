/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -> CommitProcessor ->
 * FinalRequestProcessor
 * 
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
// follower服务器
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(FollowerZooKeeperServer.class);
    // 请求处理器
    CommitProcessor commitProcessor;
    // 同步处理器
    SyncRequestProcessor syncProcessor;

    /**
     * Pending sync requests
     * 记录SyncRquest请求
     * 添加地点{@link FollowerRequestProcessor#run()}中处理Sync请求
     */
    ConcurrentLinkedQueue<Request> pendingSyncs;
    
    /**
     * @param port
     * @param dataDir
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory,QuorumPeer self,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, treeBuilder, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower(){
        return self.follower;
    }

    /**
     * 设置请求执行链条
     * 1.FollowerRequestProcessor -> CommitProcessor -> FinalRequestProcessor
     * 2.SyncRequestProcessor -> SendAckRequestProcessor
     */
    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor,
                Long.toString(getServerId()), true,
                getZooKeeperServerListener());
        commitProcessor.start();
        // 链条1：FollowerRequestProcessor -->CommitProcessor -->FinalRequestProcessor
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        // 链条2：用于将propose写入磁盘 SyncRequestProcessor-->SendAckRequestProcessor
        syncProcessor = new SyncRequestProcessor(this,
                new SendAckRequestProcessor((Learner)getFollower()));
        syncProcessor.start();
    }

    /**
     * 记录收到的Leader.PROPOSAL类型数据包,也就是提案.此时的提案还未进行commit操作
     * 放请求:{@link org.apache.zookeeper.server.quorum.FollowerZooKeeperServer#logRequest}
     * 取请求:{@link org.apache.zookeeper.server.quorum.FollowerZooKeeperServer#commit}
     */
    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    /**
     * 处理leader发送过来的事务请求
     * 该方法别调用的地方包括两处
     * {@link Follower#processPacket(org.apache.zookeeper.server.quorum.QuorumPacket)} follower接收到leader的Leader.PROPOSAL类型数据包
     * {@link Learner#syncWithLeader(long)} follower与leader进行数据同步
     * @param hdr 事务请求头
     * @param txn 事务请求实体
     */
    public void logRequest(TxnHeader hdr, Record txn) {
        // 根据hdr和txn构建一个请求
        /**
         * 注意细节:构建Request的第一个参数cnxn赋值为了null
         * 为什么这样?参考{@link org.apache.zookeeper.server.FinalRequestProcessor.processRequest}
         * 避免集群中多个服务都对同一个客户端进行响应
         */
        Request request = new Request(null, hdr.getClientId(), hdr.getCxid(),
                hdr.getType(), null, null);
        request.hdr = hdr;
        request.txn = txn;
        request.zxid = hdr.getZxid();
        // 判断是否是一个有效的事务请求,如果是则将该事务请求记录到pendingTxns集合的尾部
        // 等待后续commit操作
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        // 处理事务请求,流程SyncRequestProcessor -> SendAckRequestProcessor
        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called, 
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    /**
     * 根据zxid对之前收到的提案进行commit操作
     * @param zxid
     */
    public void commit(long zxid) {
        // 1.没有待commit操作的提案,不继续处理
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid)
                    + " without seeing txn");
            return;
        }
        // 2.非阻塞的方式获取pendingTxns队列中的头元素,只是peek一下并未出队
        long firstElementZxid = pendingTxns.element().zxid;
        // 如果队首元素的zxid不等于需要提交的zxid，则退出程序
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                    + " but next pending txn 0x"
                    + Long.toHexString(firstElementZxid));
            System.exit(12);
        }
        // 3.移除pendingTxns队列中的头元素
        Request request = pendingTxns.remove();
        // 4.提交该请求，继续后续处理流程CommitProcessor -> FinalRequestProcessor
        commitProcessor.commit(request);
    }

    // 处理Leader.SYNC请求，提交待处理的请求
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){
            LOG.warn("Not expecting a sync.");
            return;
        }
        // 直接从pendingSyncs队列中获取队首元素，处理流程CommitProcessor -> FinalRequestProcessor
        Request r = pendingSyncs.remove();
		commitProcessor.commit(r);
    }
             
    @Override
    public int getGlobalOutstandingLimit() {
        return super.getGlobalOutstandingLimit() / (self.getQuorumSize() - 1);
    }
    
    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        try {
            super.shutdown();
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            if (syncProcessor != null) {
                syncProcessor.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception in syncprocessor shutdown",
                    e);
        }
    }
    
    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }
}
