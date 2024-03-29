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

import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the control logic for the Leader.
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    
    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }
    // 提案
    static public class Proposal {
        // 数据包
        public QuorumPacket packet;
        // 记录已经处理该提案并返回ack的learner的sid集合
        public HashSet<Long> ackSet = new HashSet<Long>();
        // 数据包中对应的请求
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }
    // zk服务器,构造方法中初始化
    final LeaderZooKeeperServer zk;
    // 当前集群对象
    final QuorumPeer self;
    // VisibleForTesting
    // 是否已有过半参与者确认当前leader并且完成同步
    protected boolean quorumFormed = false;
    
    // the follower acceptor thread
    // 接受来自learner的请求连接,为每个learner创建一个LearnerHandler
    // 用于后续通信
    LearnerCnxAcceptor cnxAcceptor;
    
    // list of all the followers
    // 记录所有learner的处理线程
    private final HashSet<LearnerHandler> learners =
        new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    // 记录所有follower的处理线程
    private final HashSet<LearnerHandler> forwardingFollowers =
        new HashSet<LearnerHandler>();

    /**
     * 提案统计类
     * 参考 {@link Leader#propose(org.apache.zookeeper.server.Request)}
     */
    private final ProposalStats proposalStats;

    public ProposalStats getProposalStats() {
        return proposalStats;
    }

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }
    // 记录所有observer的处理线程
    private final HashSet<LearnerHandler> observingLearners =
        new HashSet<LearnerHandler>();
        
    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    /**
     * 记录收到的follower或observer发送过来的LearnerSyncRequest请求
     * 对应的是客户端的Sync命令
     * 添加参考{@link org.apache.zookeeper.server.quorum.Leader#processSync}
     * 移除参考{@link Leader#processAck(long, long, java.net.SocketAddress)}
     */
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs =
        new HashMap<Long,List<LearnerSyncRequest>>();
    
    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * Adds peer to the leader.
     * 
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     * 
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);            
        }        
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }        
    }

    // 与learner节点的socket连接服务端
    // 构造方法中初始化
    ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new ProposalStats();
        try {
            // 初始化socket连接服务端
            if (self.getQuorumListenOnAllIPs()) {
                ss = new ServerSocket(self.getQuorumAddress().getPort());
            } else {
                ss = new ServerSocket();
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk=zk;
    }

    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;
    
    /**
     * This is for follower to truncate its logs 
     */
    final static int TRUNC = 14;
    
    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;
    
    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
        
    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;
    /**
     * 已经提交给集群中其他成员但是还没有ACK的提案 key是提案中请求的zxid,value是提案Proposal
     * 1.放的地方参考:{@link Leader#propose(org.apache.zookeeper.server.Request)}
     * 上面方法的调用参考
     * {@link ProposalRequestProcessor#processRequest(org.apache.zookeeper.server.Request)}
     * 2.取的地方参考:{@link Leader#processAck(long, long, java.net.SocketAddress)}
     */
    ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    /**
     * 记录以及被过半learner接收到的请求,但是还未被FinalRequestProcessor处理添加到database的请求
     * 参考 {@link Leader#processAck(long, long, java.net.SocketAddress)}
     *
     * 放Request地方参考
     * {@link org.apache.zookeeper.server.quorum.Leader#processAck(long, long, java.net.SocketAddress)}
     * 取Request地点参考
     * {@link org.apache.zookeeper.server.quorum.Leader.ToBeAppliedRequestProcessor#processRequest(org.apache.zookeeper.server.Request)}
     */
    ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();
    // new leader提案
    Proposal newLeaderProposal = new Proposal();

    // 负责处理与learner的连接请求
    class LearnerCnxAcceptor extends ZooKeeperThread{
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    try{
                        // 等待learner的连接请求，这里默认监听的是2888端口
                        Socket s = ss.accept();
                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        // 针对每个learner的请求都建立一个LearnerHandler与之对应
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e){
                        LOG.error("Exception while connecting to quorum learner", e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e);
            }
        }
        
        public void halt() {
            stop = true;
        }
    }
    // leader节点的状态信息
    StateSummary leaderStateSummary;
    // 默认-1
    long epoch = -1;
    // 是否在等待new epoch
    boolean waitingForNewEpoch = true;
    // 当前leader服务器是否
    volatile boolean readyToStart = false;
    
    /**
     * This method is main function that is called to lead
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    // 选举完毕后,成为leader节点的入口
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {}", electionTimeTaken);
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick.set(0);
            // 加载内存最新数据
            zk.loadData();
            // 生成leader节点的状态信息
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // Start thread that waits for connection requests from 
            // new followers.

            /* 1.启动socket服务等待learner的连接 */

            // 1.创建并启动LearnerCnxAcceptor接收来自learner的连接请求以及后续其他操作命令
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();
            
            readyToStart = true;
            /* 2.获取集群中最大的astAcceptedEpoch,然后+1更新到epoch中 */
            // 获取集群中最大的lastAcceptedEpoch,然后+1更新到epoch中
            // 这个方法会阻塞住并返回最新的epoch
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());
            // 根据最新的epoch,更新自己的zxid
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));
            synchronized(this){
                lastProposed = zk.getZxid();
            }
            // 3.创建一个NEWLEADER数据包,包含自己当前的zxid
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                    null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }
            // 4.等待过半机器(Learner和leader)针对Leader发出的LEADERINFO回复ACKEPOCH
            waitForEpochAck(self.getId(), leaderStateSummary);
            self.setCurrentEpoch(epoch);

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            try {
                // 5.等到有过半的参与者针对Leader发出的NEWLEADER返回ACK
                waitForNewLeaderAck(self.getId(), zk.getZxid());
            } catch (InterruptedException e) {
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + getSidSetString(newLeaderProposal.ackSet) + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();
                for (LearnerHandler f : learners)
                    followerSet.add(f.getSid());
                    
                if (self.getQuorumVerifier().containsQuorum(followerSet)) {
                    LOG.warn("Enough followers present. "
                            + "Perhaps the initTicks need to be increased.");
                }
                Thread.sleep(self.tickTime);
                self.tick.incrementAndGet();
                return;
            }
            // 6.启动zk服务器
            startZkServer();
            
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             * 
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }
            
            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.cnxnFactory.setZooKeeperServer(zk);
            }
            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;
            // while循环不断的进行ping,检查learner是否活跃
            while (true) {
                Thread.sleep(self.tickTime / 2);
                if (!tickSkip) {
                    self.tick.incrementAndGet();
                }
                HashSet<Long> syncedSet = new HashSet<Long>();

                // lock on the followers when we use it.
                syncedSet.add(self.getId());

                for (LearnerHandler f : getLearners()) {
                    // Synced set is used to check we have a supporting quorum, so only
                    // PARTICIPANT, not OBSERVER, learners should be used
                    if (f.synced() && f.getLearnerType() == LearnerType.PARTICIPANT) {
                        syncedSet.add(f.getSid());
                    }
                    f.ping();
                }

                // check leader running status
                if (!this.isRunning()) {
                    shutdown("Unexpected internal error");
                    return;
                }

              if (!tickSkip && !self.getQuorumVerifier().containsQuorum(syncedSet)) {
                //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                    // Lost quorum, shutdown
                    shutdown("Not sufficient followers synced, only synced with sids: [ "
                            + getSidSetString(syncedSet) + " ]");
                    // make sure the order is the same!
                    // the leader goes to looking
                    return;
              } 
              tickSkip = !tickSkip;
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }
        
        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }
        
        // NIO should not accept conenctions
        self.cnxnFactory.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     *
     * @param sid learner的sid
     * @param zxid 针对这个请求分配的zxid
     *                the zxid of the proposal sent out
     * @param followerAddr learner的Socket信息
     */
    /**
     * 处理接收到的follower和leader自己的Leader.ACK类型数据包
     * leader自己调用这个方法参考{@link org.apache.zookeeper.server.quorum.AckRequestProcessor#processRequest(org.apache.zookeeper.server.Request)}
     * @param sid
     * @param zxid
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack by this method. However,
             * the learner sends ack back to the leader after it gets UPTODATE
             * so we just ignore the message.
             */
            return;
        }
        // 虽然接收到learner的ACK数据包,但是此时没有等到ACK的请求,不继续处理
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        // 虽然接收到follower的ACK数据包,但是数据包中的zxid小于当前leader已经发送出去的commit请求的zxid, 不继续处理
        // 因为leader针对当前zxid已经收到过半follower的处理了
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        // 获取zxid对应的提案,该提案是等待过半follower ACK的提案
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }
        // 将learner的sid记录到Proposal提案自己的ackSet中
        // ackSet用来记录已经收到的ack的服务的sid
        p.ackSet.add(sid);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }
        // 判断收到的ack是否过半
        if (self.getQuorumVerifier().containsQuorum(p.ackSet)){
            /* 进入到这里说明该提案已经被过半learner接收到 */
            if (zxid != lastCommitted+1) {
                LOG.warn("Commiting zxid 0x{} from {} not first!",
                        Long.toHexString(zxid), followerAddr);
                LOG.warn("First is 0x{}", Long.toHexString(lastCommitted + 1));
            }
            // 从outstandingProposals队列中移除该提案
            // 因为该提案已经被过半learner接收到
            outstandingProposals.remove(zxid);
            if (p.request != null) {
                // 记录到toBeApplied集合
                toBeApplied.add(p);
            }

            if (p.request == null) {
                LOG.warn("Going to commmit null request for proposal: {}", p);
            }
            // 发送Leader.COMMIT请求给各个follower
            commit(zxid);
            // 发送Leader.INFORM请求给observer
            inform(p);
            // 将请求传递给CommitProcessor唤醒里面的等待
            zk.commitProcessor.commit(p.request);
            // 如果当前请求在进行过半投票时,有对应的LearnerSyncRequest命令那么执行该命令
            if(pendingSyncs.containsKey(zxid)){
                for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                    sendSync(r);
                }
            }
        }
    }

    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private RequestProcessor next;

        /**
         * 赋值地点参考
         * {@link LeaderZooKeeperServer#setupRequestProcessors()}
         * 该属性为Leader.toBeApplied属性的引用
         *
         * 放Request地方参考
         * {@link org.apache.zookeeper.server.quorum.Leader#processAck(long, long, java.net.SocketAddress)}
         * 取Request地点参考
         * {@link ToBeAppliedRequestProcessor#processRequest(org.apache.zookeeper.server.Request)}
         */
        private ConcurrentLinkedQueue<Proposal> toBeApplied;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         * 
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next,
                ConcurrentLinkedQueue<Proposal> toBeApplied) {
            // 注意这里,如果nextProcessor不是FinalRequestProcessor立即报错
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.toBeApplied = toBeApplied;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        // 处理请求
        public void processRequest(Request request) throws RequestProcessorException {
            // request.addRQRec(">tobe");
            // 首先将请求交给FinalRequestProcessor来进行同步处理
            next.processRequest(request);
            // 最后获取toBeApplied头位置中的提案,如果和请求的zxid一致则从toBeApplied中删除
            Proposal p = toBeApplied.peek();
            if (p != null && p.request != null
                    && p.request.zxid == request.zxid) {
                toBeApplied.remove();
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     * 
     * @param qp
     *                the packet to be sent
     */
    /**
     * 将数据包提交给各个learner的LearnerHandler线程来处理,此时并不包含Leader自己
     *
     * forwardingFollowers初始化流程如下
     * 1.{@link org.apache.zookeeper.server.quorum.LearnerHandler#run}
     *                      ↓↓↓
     * 2.{@link org.apache.zookeeper.server.quorum.Leader#startForwarding}
     *                      ↓↓↓
     * 3.{@link org.apache.zookeeper.server.quorum.Leader#addForwardingFollower}
     * @param qp
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {                
                f.queuePacket(qp);
            }
        }
    }
    
    /**
     * send a packet to all observers     
     */
    void sendObserverPacket(QuorumPacket qp) {        
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    /**
     * 最后一个发送commit的请求
     * 参考{@link Leader#commit(long)}
     */
    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 发送commit请求给集群中的follower
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized(this){
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }
    
    /**
     * Create an inform packet and send it to all observers.
     * @param zxid
     * @param proposal
     */
    public void inform(Proposal proposal) {   
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid, 
                                            proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    /**
     * 最后一次提案中的zxid
     * 参考{@link Leader#propose(org.apache.zookeeper.server.Request)}
     */
    long lastProposed;

    
    /**
     * Returns the current epoch of the leader.
     * 
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }
    
    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     * 
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    // 创建一个提案然后提交给集群中的其他成员
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }
        /* 1. 序列化请求封装为QuorumPacket数据包 */
        // 1.1 将请求序列化为byte数组
        byte[] data = SerializeUtils.serializeRequest(request);
        // 1.2 统计提案中请求的大小
        proposalStats.setLastProposalSize(data.length);
        // 1.3 封装为一个请求数据包
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);
        // 1.4 构建Proposal
        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }
            // 记录最后提案的zxid
            lastProposed = p.packet.getZxid();
            // 将提案记录到outstandingProposals集合中
            outstandingProposals.put(lastProposed, p);
            // 同步follower对应的LearnerHandler同步数据
            sendPacket(pp);
        }
        return p;
    }
            
    /**
     * Process sync requests
     * 
     * @param r the request
     */
    
    synchronized public void processSync(LearnerSyncRequest r){
        // 如果没有等待ACK的请求
        // 那么执行sendSync()方法,构建一个Leader.SYNC请求发给learner
        if(outstandingProposals.isEmpty()){
            sendSync(r);
        // 有等待ACK的请求,也就是leader将请求发送给了follower正在等待过半的回复
        } else {
            // 获取最后发送出去的请求的zxid,然后与当前的LearnerSyncRequest进行绑定记录到pendingSyncs集合中
            // 当对应的zxid收到过半的回复后,在从pendingSyncs集合中获取对应的LearnerSyncRequest请求并处理
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }
        
    /**
     * Sends a sync message to the appropriate server
     * 
     * @param f
     * @param r
     */
            
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }
                
    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     * 
     * @param handler handler of the follower
     * @param lastSeenZxid learner最后处理提案的zxid
     * @return last proposed zxid
     */
    synchronized public long startForwarding(LearnerHandler handler,
            long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        // 如果当前leader最后Proposal的Proposed的zxid > lastSeenZxid
        // 说明有Proposal要提给learner
        if (lastProposed > lastSeenZxid) {
            // 将已经被过半follower处理并回应ACK的Proposal添加到queuedPackets队列
            // 这些提案还未进入database,所以在前面的比较环节是不包含这些提案的
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                // 将Proposal添加到queuedPackets队列
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                // 对应Proposal添加一个Leader.COMMIT类型的QuorumPacket到queuedPackets队列
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            // 将已经提交给集群中其他成员但是还没有ACK的Proposal添加到queuedPackets队列
            // 这里判断了LearnerHandler对应的learner的类型,只发送给follower,不发送给observer
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        // 记录follower或observer的处理线程
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }
                
        return lastProposed;
    }
    // VisibleForTesting
    // 记录集群中follower和leader的sid
    protected Set<Long> connectingFollowers = new HashSet<Long>();
    // 该方法会在Leader.lead()和LearnerHandler.run()方法中被调用
    // 获取集群中最大的lastAcceptedEpoch,然后+1更新到epoch中
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized(connectingFollowers) {
            // 1.是否在等待新的epoch,不在等待时直接返回
            if (!waitingForNewEpoch) {
                return epoch;
            }
            // 2.如果参数lastAcceptedEpoch超过自己当前的epoch,那么将参数lastAcceptedEpoch + 1赋值给epoch
            // 也就是在不断的将集群中最大的epoch + 1操作
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch+1;
            }
            // 3.如果参数sid是集群的一部分,则记录到connectingFollowers集合中
            // 并且类型为sid所属服务的server.type == LearnerType.PARTICIPANT才加入connectingFollowers集合中
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            // 4.connectingFollowers集合中已包含自己说明自己已经在集群中并且
            // 也包含过半的机器了,说明当前已经过半的集群达成了epoch
            // 那么就不需要在继续等待
            /**
             * 不明白为什么需要执行connectingFollowers.contains(self.getId())判断可以
             * 参考{@link org.apache.zookeeper.server.quorum.Leader#lead}或当前方法的注释
             */
            if (connectingFollowers.contains(self.getId()) && 
                                            verifier.containsQuorum(connectingFollowers)) {
                // 更新等待标识
                waitingForNewEpoch = false;
                // 设置最新的acceptedEpoch
                self.setAcceptedEpoch(epoch);
                // 唤醒等待的线程,这里有可能其他learner注册的时候发生并发,导致阻塞在入口synchronized上
                connectingFollowers.notifyAll();
            } else {// 5.连接的集群未过半,那么等待一段时间后直接返回
                // 获取开始时间,结束时间
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                // while循环直到超时后直接退出
                while(waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                // while循环退出后校验,如果还没有过半集群连接上,那么抛出异常
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");        
                }
            }
            return epoch;
        }
    }
    // VisibleForTesting
    // 记录针对Leader发出的LEADERINFO回复ACKEPOCH的follower和leader服务的sid
    protected Set<Long> electingFollowers = new HashSet<Long>();
    // VisibleForTesting
    // 记录是否已有过半的(learner针对Leader发出的LEADERINFO回复ACKEPOCH
    protected boolean electionFinished = false;
    // 等待过半机器(Learner和leader)针对Leader发出的LEADERINFO回复ACKEPOCH
    // 该方法会被leader.lead()和LearnerHandler.run()方法调用
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized(electingFollowers) {
            // 已经收到过半的learner回复ACKEPOCH
            if (electionFinished) {
                return;
            }
            /**
             * 如果收到learner的CurrentEpoch为-1
             * 说明在上一步进行newEpoch计算的时候,learner的AcceptedEpoch与leader的epoch一致
             * 这种情况应该属于集群中其他learner的AcceptedEpoch <= leader的epoch,所以导致leader的epoch并没有发生更新
             * 但是收到learner的CurrentEpoch不为-1
             * 说明集群中有的服务的AcceptedEpoch要高于leader的epoch,导致leader的epoch发生更新
             * 由于learner在收到LEADERINFO类型的QuorumPacket会响应自己的CurrentEpoch和LastZxid,所以这里要比对一下
             * 1.learner的currentEpoch > leader的currentEpoch
             * 2.learner的currentEpoch = leader的currentEpoch && earner的lastZxid > leader的lastZxid
             * 以上两种情况任一发生都要抛出异常,因为按照选举的优先级来说这是不可能出现的
             * 选举优先级策略参考{@link org.apache.zookeeper.server.quorum.FastLeaderElection.totalOrderPredicate}
             */

            /**
             * 同时在注意一点
             * 只有learner的CurrentEpoch不为-1才会进入if分支并加入electingFollowers集合中
             * electingFollowers集合用来决定当前阻塞是否阻塞以及唤醒被阻塞的LearnerHandler
             * 这里也就明白了{@link Learner#registerWithLeader(int)}方法中341--344代码的注释
             */
            if (ss.getCurrentEpoch() != -1) {
                // 校验
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                // 校验,记录sid
                // 并且类型为sid所属服务的server.type == LearnerType.PARTICIPANT才加入electingFollowers集合中
                if (isParticipant(id)) {
                    electingFollowers.add(id);
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            // 校验是否已过半机器针对Leader发出的LEADERINFO回复ACKEPOCH
            /**
             * 不明白为什么需要执行electingFollowers.contains(self.getId())判断可以
             * 参考{@link org.apache.zookeeper.server.quorum.Leader#lead}或当前方法的注释
             */
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                // 唤醒被阻塞的所有LearnerHandler,因为其他LearnerHandler都阻塞在了else分支下面
                electingFollowers.notifyAll();
            } else {// 如果没有过半的集群针对Leader发出的LEADERINFO回复ACKEPOCH
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                // 循环等待,直到过半或超时
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string  
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     */
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + getSidSetString(newLeaderProposal.ackSet)
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     *
     * @param sid
     * @throws InterruptedException
     */
    // 该方法会被leader.lead()和LearnerHandler.run()方法调用
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.ackSet) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            if (isParticipant(sid)) {
                newLeaderProposal.ackSet.add(sid);
            }

            if (self.getQuorumVerifier().containsQuorum(
                    newLeaderProposal.ackSet)) {
                quorumFormed = true;
                newLeaderProposal.ackSet.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.ackSet.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        default:
            return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getVotingView().containsKey(sid);
    }
}
