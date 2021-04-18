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

    /*
     * Pending sync requests
     */
    // 等待同步的请求(sync 请求)
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
    // 等待处理的事务请求
    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    // 记录事务请求到等待队列
    public void logRequest(TxnHeader hdr, Record txn) {
        Request request = new Request(null, hdr.getClientId(), hdr.getCxid(),
                hdr.getType(), null, null);
        request.hdr = hdr;
        request.txn = txn;
        request.zxid = hdr.getZxid();
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);
        }
        // 提交该请求，处理流程SyncRequestProcessor -> SendAckRequestProcessor
        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called, 
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    // 提交zxid对应的请求(pendingTxns.element()也就是获取pendingTxns队列首元素)
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid)
                    + " without seeing txn");
            return;
        }
        // 获取pendingTxns队列的首元素，只是查看并不出队
        long firstElementZxid = pendingTxns.element().zxid;
        // 如果队首元素的zxid不等于需要提交的zxid，则退出程序
        if (firstElementZxid != zxid) {
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                    + " but next pending txn 0x"
                    + Long.toHexString(firstElementZxid));
            System.exit(12);
        }
        // 移除队首元素并返回
        Request request = pendingTxns.remove();
        // 提交该请求，处理流程CommitProcessor -> FinalRequestProcessor
        commitProcessor.commit(request);
    }

    // 处理Leader.SYNC请求，提交待处理的请求
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){
            LOG.warn("Not expecting a sync.");
            return;
        }
        // 直接从pendingSyncs队列中获取队首元素，处理流程  CommitProcessor -> FinalRequestProcessor
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
