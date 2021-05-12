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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
// LearnerHandler实例都对应一个Leader与Learner服务器之间的连接，
// 其负责Leader和Learner服务器之间几乎所有的消息通信和数据同步
public class LearnerHandler extends ZooKeeperThread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);
    // 已learner建立的socket连接
    protected final Socket sock;    

    public Socket getSocket() {
        return sock;
    }
    // leader服务
    final Leader leader;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    // learner下一个ack的超时时间
    volatile long tickOfNextAckDeadline;
    
    /**
     * ZooKeeper server identifier of this learner
     */
    // learner的sid
    protected long sid = 0;
    
    long getSid(){
        return sid;
    }                    
    // learner连接后,传递过来的版本,写死的0x10000
    protected int version = 0x1;
    
    int getVersion() {
    	return version;
    }
    
    /**
     * The packets to be sent to the learner
     */
    // 记录需要发送给learner的集群数据包
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
        new LinkedBlockingQueue<QuorumPacket>();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
             if (currentZxid == zxid) {
                 currentTime = nextTime;
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();
    // zk自定义的输入流
    private BinaryInputArchive ia;
    // zk自定义的输出流
    private BinaryOutputArchive oa;
    // 输入流
    private final BufferedInputStream bufferedInput;
    // 输出流
    private BufferedOutputStream bufferedOutput;

    /**
     * 当leader收到一个learner的socket连接时就对应创建一个LearnerHandler
     * @param sock 接收到的socket对象
     * @param bufferedInput socket输入流
     * @param leader leader对象
     * @throws IOException
     */
    LearnerHandler(Socket sock, BufferedInputStream bufferedInput,
                   Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        this.bufferedInput = bufferedInput;
        try {
            leader.self.authServer.authenticate(sock,
                    new DataInputStream(bufferedInput));
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection",
                    sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();
    // 记录learner的类型
    private LearnerType  learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     * 将queuedPackets队列中的数据包获取出来并发送给learner
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                // 非阻塞的方式获取queuedPackets队列中的一个请求
                QuorumPacket p;
                p = queuedPackets.poll();
                // queuedPackets已经没有任何请求,采用阻塞的方式获取queuedPackets队列中的一个请求
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }
                // 校验LearnerHandler是否已经关闭
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                // 最后发送数据包
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type = null;
        String mess = null;
        Record txn = null;
        
        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;    
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    // 这里源码可结合org.apache.zookeeper.server.quorum.Learner.registerWithLeader()一起分析
    @Override
    public void run() {
        try {
            /**
             * 1.基于learner的连接构建输入输出流
             */

            // 记录新添加的learner，最终走到下面会是一个while循环
            leader.addLearnerHandler(this);
            tickOfNextAckDeadline = leader.self.tick.get()
                    + leader.self.initLimit + leader.self.syncLimit;
            // 构建zk自定义的输入/输出流
            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            /**
             * 2.读取learner发送过来的包含它们自身信息的QuorumPacket
             * 关联代码{@link org.apache.zookeeper.server.quorum.Learner.registerWithLeader}
             */

            // 2.1 解析packet标签对应的QuorumPacket
            QuorumPacket qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            // 2.2 校验learner的角色
            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){
            	LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            // 2.3 解析请求中携带的LearnerInfo对象,包含了learner的sid以及写死的版本号0x10000
            byte learnerInfoData[] = qp.getData();
            if (learnerInfoData != null) {
                // 2.3.1如果数据包长度为8个字节,那么只包含一个long sid
            	if (learnerInfoData.length == 8) {
            		ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
            		this.sid = bbsid.getLong();
            	} else {// 2.3.2否则说明包含sid和version(我们这里就是走的这个)
            		LearnerInfo li = new LearnerInfo();
            		ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
            		this.sid = li.getServerid();
            		// version在learner写死的0x10000
            		this.version = li.getProtocolVersion();
            	}
            } else {
            	this.sid = leader.followerCounter.getAndDecrement();
            }

            LOG.info("Follower sid: " + sid + " : info : "
                    + leader.self.quorumPeers.get(sid));
            // 2.4 从请求中读取learner的角色类型并更新对应的LearnerHandler中
            if (qp.getType() == Leader.OBSERVERINFO) {
                    //  PARTICIPANT或OBSERVER
                  learnerType = LearnerType.OBSERVER;
            }            
            // 2.5 从请求中读取learner的zxid中的acceptedEpoch
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
            // 记录learner最后处理的zxid
            long peerLastZxid;
            // 记录learner的统计信息
            StateSummary ss = null;
            // 2.6 从请求中读取learner的zxid
            long zxid = qp.getZxid();
            // 2.7 根据从learner发送过来的请求中的lastAcceptedEpoch更新leader的epoch
            // leader的epoch会从集群中的所有lastAcceptedEpoch中获取一个最大值在+1,作为整个集群新的epoch
            // getEpochToPropose()方法会阻塞,最后返回集群新的epoch
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);

            /* 这里的逻辑忽略 */
            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                // 封装一个假消息进行处理
                leader.waitForEpochAck(this.getSid(), ss);
            /* 由于learner的版本为0x10000所以走这里的逻辑 */
            } else {
                // 这里结合org.apache.zookeeper.server.quorum.Learner.registerWithLeader()方法分析

                /* 3 leader发送LEADERINFO数据包并等待learner的ACKEPOCH的响应 */

                // 3.1 构建LEADERINFO数据包,版本为写死的0x10000以及基于最新的epoch构建的zxid
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                // 3.2 发送数据包给learner,标签为packet
                oa.writeRecord(newEpochPacket, "packet");
                bufferedOutput.flush();

                /* 4.等待并解析learner对LEADERINFO数据包的响应ACKEPOCH */

                // 解析learner的ACKEPOCH数据包
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                // 响应的数据包类型不为ACKEPOCH
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				}
                // 读取learner发送过来的它自身的currentEpoch
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                // 封装learner的一个状态摘要,注意bbepoch.getInt()该值可能为-1
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                // 等待过半机器(Learner和leader)针对Leader发出的LEADERINFO回复ACKEPOCH
                leader.waitForEpochAck(this.getSid(), ss);
            }

            /* 5.准备与learner进行数据同步 */

            // 5.1.获取learner节点最后处理的zxid,该值将作为同步数据的锚点
            peerLastZxid = ss.getLastZxid();

            //a. committedLog里面保存着Leader处理的最新的最多500个Proposal
            //b. 当learner处理的Proposal > maxCommittedLog, 则learner要TRUNC自己的Proposal至maxCommittedLog
            //c. 当learner处理的Proposal <= maxCommittedLog && >=minCommittedLog, 则Leader将learner没有的Proposal发送到learner
            //d. 当learner处理的Proposal < minCommittedLog, 则Leader发送Leader.SNAP给learner,并且将自身的数据序列化成数据流,发送给learner

            /* the default to send to the follower */
            // 初始化发送给learner的数据包类型
            int packetToSend = Leader.SNAP;
            // 数据同步时,发送给learner的首个数据包的zxid
            long zxidToSend = 0;
            // 要发送给learner的leader节点的最大的zxid
            long leaderLastZxid = 0;
            /** the packets that the follower needs to get updates from **/
            // learner需要从该位置+1的位置同步数据
            long updates = peerLastZxid;
            
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */
            // 5.2.锁定内存数据库,准备进行数据同步
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();
            ReadLock rl = lock.readLock();
            try {
                // 锁上内存数据库
                rl.lock();
                // 5.2.1 获取minCommittedLog,和maxCommittedLog
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog();
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();
                LOG.info("Synchronizing with Follower sid: " + sid
                        +" maxCommittedLog=0x"+Long.toHexString(maxCommittedLog)
                        +" minCommittedLog=0x"+Long.toHexString(minCommittedLog)
                        +" peerLastZxid=0x"+Long.toHexString(peerLastZxid));
                // 5.2.2 获取最近已提交的提案,最多500个
                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog();
                // 5.2.3 learner节点与leader节点的zxid相等,数据一致不需要同步
                // 修改返回数据包类型为DIFF
                // 更新zxidToSend为peerLastZxid,表示已同步到learner的zxid
                if (peerLastZxid == leader.zk.getZKDatabase().getDataTreeLastProcessedZxid()) {
                    // Follower is already sync with us, send empty diff
                    LOG.info("leader and follower are in sync, zxid=0x{}",
                            Long.toHexString(peerLastZxid));
                    packetToSend = Leader.DIFF;
                    zxidToSend = peerLastZxid;
                // 5.2.4 learner与leader的zxid不一致 && leader已提交的提案信息不为空
                } else if (proposals.size() != 0) {
                    LOG.debug("proposal size is {}", proposals.size());
                    // 5.2.4.1 learner的zxid介于[minCommittedLog,maxCommittedLog]之间,learner的数据少于leader,需要同步
                    // 此时需要进行提案的遍历,查找未同步提案中的第一个,针对该提案的上一个提案的zxid(prevProposalZxid)存在两种情况
                    // a.prevProposalZxid < learner最大的zxid
                    //      修改返回数据包类型为TRUNC
                    //      更新zxidToSend为prevProposalZxid
                    //      封装提案为new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(), null, null)并提交到queuedPackets集合
                    // b.prevProposalZxid = learner最大的zxid
                    //      修改返回数据包类型为DIFF
                    //      更新zxidToSend为maxCommittedLog
                    //      封装提案为new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(), null, null)并提交到queuedPackets集合
                    if ((maxCommittedLog >= peerLastZxid)
                            && (minCommittedLog <= peerLastZxid)) {
                        LOG.debug("Sending proposals to follower");

                        // as we look through proposals, this variable keeps track of previous
                        // proposal Id.
                        // 在遍历提案时,记录上一个提案的zxid
                        long prevProposalZxid = minCommittedLog;

                        // Keep track of whether we are about to send the first packet.
                        // Before sending the first packet, we have to tell the learner
                        // whether to expect a trunc or a diff
                        // 当出现第一个learner不知道的提案时,标记为false
                        boolean firstPacket=true;

                        // If we are here, we can use committedLog to sync with
                        // follower. Then we only need to decide whether to
                        // send trunc or not
                        // 修改返回数据包类型为DIFF
                        packetToSend = Leader.DIFF;
                        zxidToSend = maxCommittedLog;

                        for (Proposal propose: proposals) {
                            // skip the proposals the peer already has
                            // 该提案在learner上已经处理过,跳过继续处理下一条并更新prevProposalZxid
                            if (propose.packet.getZxid() <= peerLastZxid) {
                                prevProposalZxid = propose.packet.getZxid();
                                continue;
                            } else {
                                // 执行到这里说明当前这个提案follower还没处理过
                                // If we are sending the first packet, figure out whether to trunc
                                // in case the follower has some proposals that the leader doesn't
                                // 如果当前提案是遇到的第一个未处理的提案,那么判断该提案的上一个提案的zxid是否小于learner的last zxid
                                // 如果小于,说明当前提案的zxid > peerLastZxid(learner最后处理的提案) && prevProposalZxid(learner已知的提案) < peerLastZxid(learner最后处理的提案)
                                // 比如当前提案5,peerLastZxid为4,prevProposalZxid为3,当前zk服务丢了数据
                                // 修改返回数据包类型为TRUNC,让learner截取提案到prevProposalZxid位置
                                if (firstPacket) {
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) {
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC;                                        
                                        zxidToSend = prevProposalZxid;
                                        // 更新updates表示learner需要从该提案的下一个提案进行同步
                                        updates = zxidToSend;
                                    }
                                }
                                // 将提案添加到发送队列
                                queuePacket(propose.packet);
                                // 发送一个COMMIT请求,让learner来处理这个提案
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(),
                                        null, null);
                                // 只是缓存到queuedPackets集合中
                                queuePacket(qcommit);
                            }
                        }
                    // 5.2.4.2 learner节点的zxid要高于leader节点的maxCommittedLog
                    // 说明learner提案多余leader,修改返回数据包类型为TRUNC
                    //      修改返回数据包类型为TRUNC
                    //      更新zxidToSend为maxCommittedLog
                    } else if (peerLastZxid > maxCommittedLog) {
                        LOG.debug("Sending TRUNC to follower zxidToSend=0x{} updates=0x{}",
                                Long.toHexString(maxCommittedLog),
                                Long.toHexString(updates));

                        packetToSend = Leader.TRUNC;
                        zxidToSend = maxCommittedLog;
                        // 更新updates表示learner需要从该提案的下一个提案进行同步
                        updates = zxidToSend;
                    } else {
                        LOG.warn("Unhandled proposal scenario");
                    }
                } else {
                    // just let the state transfer happen
                    LOG.debug("proposals is empty");
                }               

                LOG.info("Sending " + Leader.getPacketType(packetToSend));

                // 5.2.5 根据updates,查找toBeApplied集合中需要发送给learner的提案
                // 以及outstandingProposals集合中需要提交的提案
                leaderLastZxid = leader.startForwarding(this, updates);

            } finally {
                rl.unlock();
            }
            // 5.3 构建NEWLEADER请求添加到queuedPackets集合
             QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                    ZxidUtils.makeZxid(newEpoch, 0), null, null);
             if (getVersion() < 0x10000) {
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();
            // 5.4 封装一个请求并发送,告诉learner最新的zxidToSend以及同步数据的模式
            //Need to set the zxidToSend to the latest zxid
            if (packetToSend == Leader.SNAP) {
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            oa.writeRecord(new QuorumPacket(packetToSend, zxidToSend, null, null), "packet");
            bufferedOutput.flush();
            
            /* if we are not truncating or sending a diff just send a snapshot */
            // 5.5 同步数据类型为SNAP时,直接序列化内存数据库同步给learner
            if (packetToSend == Leader.SNAP) {
                LOG.info("Sending snapshot last zxid of peer is 0x"
                        + Long.toHexString(peerLastZxid) + " " 
                        + " zxid of leader is 0x"
                        + Long.toHexString(leaderLastZxid)
                        + "sent zxid of db as 0x" 
                        + Long.toHexString(zxidToSend));
                // Dump data to peer
                // 将内存数据同步给learner
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                // 发送快照设置一个签名
                oa.writeString("BenWasHere", "signature");
            }
            bufferedOutput.flush();
            
            // Start sending packets
            // 5.6 启动一个线程处理queuedPackets队列中的数据包
            // 也就是与learner进行数据同步
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption",e);
                    }
                }
            }.start();
            
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            // 5.7 在步骤5.3中构建了NEWLEADER数据包,在步骤5.6中会被发送出去.
            // 这里阻塞等待NEWLEADER请求的响应
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            if(qp.getType() != Leader.ACK){
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            LOG.info("Received NEWLEADER-ACK message from " + getSid());
            leader.waitForNewLeaderAck(getSid(), qp.getZxid());

            syncLimitCheck.start();
            
            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);

            /*
             * Wait until leader starts up
             */
            synchronized(leader.zk){
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            // 5.8 发送一个UPTODATE请求,表示请求处理结束
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            // 5.9 从这里开始不断的读取follower的请求
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                    // 5.9.1 learner发送回来的ACK请求
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    syncLimitCheck.updateAck(qp.getZxid());
                    // 处理follower的ack请求
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp
                            .getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        leader.zk.touch(sess, to);
                    }
                    break;
                case Leader.REVALIDATE:
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.touch(id, to);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);
                    break;
                case Leader.REQUEST:                    
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    // 处理sync的响应
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    leader.zk.submitRequest(si);
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the 
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " 
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() {
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            synchronized(leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * 添加数据包到queuedPackets集合
     * @param p
     */
    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() {
        return isAlive()
        && leader.self.tick.get() <= tickOfNextAckDeadline;
    }
}
