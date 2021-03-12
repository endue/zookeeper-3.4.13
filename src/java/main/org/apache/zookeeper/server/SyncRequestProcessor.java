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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
// 将请求以批量的方式记同步到磁盘，请求在同步到磁盘之前，不会转发到下一个处理器
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    // zk服务器
    private final ZooKeeperServer zks;
    // 请求队列
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    // 下一个处理器
    private final RequestProcessor nextProcessor;
    // 快照处理线程，用来将请求同步到磁盘
    private Thread snapInProcess = null;
    // 运行标识
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    // 等待刷入磁盘的请求
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    // 随机数生成器
    private final Random r = new Random(System.nanoTime());
    /**
     * The number of log entries to log before starting a snapshot
     */
    // 刷入磁盘快照数的
    private static int snapCount = ZooKeeperServer.getSnapCount();
    
    /**
     * The number of log entries before rolling the log, number
     * is chosen randomly
     */
    private static int randRoll;
    // 关闭当前处理器时，会设置该请求标识到queuedRequests队列中
    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }
    
    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
        randRoll = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }
    
    /**
     * Sets the value of randRoll. This method
     * is here to avoid a findbugs warning for
     * setting a static variable in an instance
     * method.
     *
     * @param roll
     */
    private static void setRandRoll(int roll) {
        randRoll = roll;
    }

    @Override
    public void run() {
        try {
            // 记录已写入到内存数的请求
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 防止集群中的所有服务在同一时刻进行数据快照
            // 生成一个随机值来决定是否需要进行快照
            setRandRoll(r.nextInt(snapCount/2));
            while (true) {
                Request si = null;
                // 没有需要刷入磁盘的请求
                if (toFlush.isEmpty()) {
                    // 从请求队列中获取一个，这个方法在queuedRequests队列为空时会阻塞住
                    si = queuedRequests.take();
                // 有需要刷入磁盘的请求
                } else {
                    // 获取请求队列中的一个请求，这个方法在queuedRequests队列为空时会返回null
                    si = queuedRequests.poll();
                    if (si == null) {// queuedRequests队列已经空了没有请求要处理了，将toFlush队列中的请求刷入磁盘
                        flush(toFlush);
                        continue;
                    }
                }
                // 当前处理器已关闭，参考shutdown()方法
                if (si == requestOfDeath) {
                    break;
                }
                // 请求不为null，处理请求
                if (si != null) {
                    // track the number of records written to the log
                    // 将请求添加到内存目录树，也就是FileTxnSnapLog
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;// 记录写入到内存目录树的日志数
                        // 确定是否需要刷内存目录树到磁盘(这里就和上面计算随机randRoll关联上了)
                        if (logCount > (snapCount / 2 + randRoll)) {
                            setRandRoll(r.nextInt(snapCount/2));
                            // roll the log
                            // 更新一个新的事务文件
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                // 创建一个线程并启动，执行内存目录树刷磁盘
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            // 重置logCount
                            logCount = 0;
                        }
                    // 请求中的hdr为null，会走到这里，也就是读请求
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    // 将请求添加到toFlush
                    toFlush.add(si);
                    // 处理propose超过1000刷磁盘
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    // 刷磁盘
    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        // 等待刷入磁盘的数据为空
        if (toFlush.isEmpty())
            return;
        // 将内存目录树中的数据刷如磁盘
        zks.getZKDatabase().commit();
        // 遍历待刷入磁盘的请求，并交给下一个请求处理
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();// 注意这里为移除
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    // 关闭
    public void shutdown() {
        LOG.info("Shutting down");
        // 设置requestOfDeath请求到队列中
        queuedRequests.add(requestOfDeath);
        try {
            // 等待当前线程执行结束，也就是run()方法
            if(running){
                this.join();
            }
            // toFlush不为空，执行flush
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        // 关闭下一个处理器
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    // 处理请求，将请求放到queuedRequests队列中
    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
