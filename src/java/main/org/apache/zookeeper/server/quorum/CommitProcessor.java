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

import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /**
     * Requests that we are holding until the commit comes in.
     */
    // 记录接收到的请求,当请求通过processRequest()方法到达该Processor时,保存到当前队列
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    // 记录收到的请求,当请求通过commit()方法到达该Processor时,保存到当前队列
    // 也就是已经被过半learner commit的请求
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;

    // run()方法将queuedRequests队列中非事务请求或者已经被过半learner处理的请求转义到该队列
    ArrayList<Request> toProcess = new ArrayList<Request>();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
            boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }
    // 是否关闭标识符
    volatile boolean finished = false;

    /**
     * 该方法在处理请求的时候会将队列中的Request赋值给局部遍历nextPending
     * 注意并没有将cnxn同步过去，这样做的原因是谁接收的请求，谁来进行响应
     */
    @Override
    public void run() {
        try {
            Request nextPending = null;            
            while (!finished) {
                /* 2.第二步在看这里处理非事务请求 */

                // 2.1 将toProcess集合中待处理的非事务请求或者已经被过半learner处理的请求
                // 传递给下一个Processor并toProcess集合
                int len = toProcess.size();
                for (int i = 0; i < len; i++) {
                    nextProcessor.processRequest(toProcess.get(i));
                }
                toProcess.clear();

                /* 3.第三步看这里处理事务请求 */

                synchronized (this) {
                    // 3.1
                    // a.没有待处理的请求(queuedRequests.size() == 0) 或者 有事务请求正在等待过半follower的ACK回复(nextPending != null)
                    // b.没有事务请求被过半follower响应了ACK(committedRequests.size() == 0)
                    // a && b成立,阻塞等待,直到被commit()或者processRequest()方法唤醒也就是有新的请求或者有已经被过半learner处理的事务请求
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            && committedRequests.size() == 0) {
                        wait();
                        continue;
                    }
                    // First check and see if the commit came in for the pending
                    // request
                    // 3.2
                    // a.没有待处理的请求(queuedRequests.size() == 0) 或者 有事务请求正在等待过半follower的ACK回复(nextPending != null)
                    // b.有事务请求被过半follower响应了ACK(committedRequests.size() > 0)
                    // a && b成立,说明有某个事务请求被过半learner处理了
                    if ((queuedRequests.size() == 0 || nextPending != null)
                            && committedRequests.size() > 0) {
                        // 3.2.1 从头位置获取已经被learner过半提交的事务请求
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        // 3.2.2 判断被过半learner处理的事务请求和当前正在等待过半learner回复的事务请求是否是同一个
                        // 如果是同一个将nextPending置为null方便继续处理后续的事务请求,然后将该事务请求添加到toProcess队列
                        // 如果不是则将请求添加到toProcess队列
                        if (nextPending != null
                                && nextPending.sessionId == r.sessionId
                                && nextPending.cxid == r.cxid) {
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);
                            nextPending = null;
                        } else {
                            // this request came from someone else so just
                            // send the commit packet
                            toProcess.add(r);
                        }
                    }
                }

                // We haven't matched the pending requests, so go back to
                // waiting
                // 有事务请求存在,不继续往下执行(因为当前事务请求还未被过半follower处理掉)
                // 所以返回继续等待
                if (nextPending != null) {
                    continue;
                }

                /* 1.先看这里处理请求分为事务和非事务两种 */

                synchronized (this) {
                    // Process the next requests in the queuedRequests
                    // 事务请求赋值给nextPending
                    // 非事务请求记录到toProcess队列
                    while (nextPending == null && queuedRequests.size() > 0) {
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                        case OpCode.create:
                        case OpCode.delete:
                        case OpCode.setData:
                        case OpCode.multi:
                        case OpCode.setACL:
                        case OpCode.createSession:
                        case OpCode.closeSession:
                            nextPending = request;
                            break;
                        case OpCode.sync:
                            if (matchSyncs) {
                                nextPending = request;
                            } else {
                                toProcess.add(request);
                            }
                            break;
                        default:
                            toProcess.add(request);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    // 收到commit请求,此时当前请求已经被过半follower处理
    synchronized public void commit(Request request) {
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            // 添加到committedRequests队列默认
            committedRequests.add(request);
            notifyAll();
        }
    }
    // 处理request，保存到queuedRequests队列中
    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        if (!finished) {
            queuedRequests.add(request);
            notifyAll();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
