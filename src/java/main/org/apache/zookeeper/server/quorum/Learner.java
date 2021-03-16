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
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication. 
 */
// 服务器的角色,该类就是follower和observer角色的父类
public class Learner {
    // 表示还在PROPOSAL阶段还未COMMIT的消息记录
    // 记录Leader发出提议，但是还没有通过过半验证的数据格式
    static class PacketInFlight {
        TxnHeader hdr;// 事物头
        Record rec;// 消息记录
    }
    // 当前集群对象
    QuorumPeer self;
    // zk服务器
    LearnerZooKeeperServer zk;
    // 与leader节点建立的输出缓冲流
    protected BufferedOutputStream bufferedOutput;
    // 与leader节点的socket连接客户端
    protected Socket sock;
    
    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    // 与leader节点建立的输入、输出流
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;  
    /** the protocol version of the leader */
    // leader的协议版本,写死的0x10000
    protected int leaderProtocolVersion = 0x01;
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);
    // 连接leader是否允许延迟
    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    // key是sessionId或者clientId
    // client连接到learner时，learner要向leader提出REVALIDATE请求，在收到回复之前，记录在一个map中，表示尚未处理完的验证
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
        new ConcurrentHashMap<Long, ServerCnxn>();
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    // session验证相关
    // 集群版client重连时调用，learner验证会话是否有效，并激活，需要发送请求给Leader
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        // 创建REVALIDATE数据包
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null);
        // 暂存到pendingRevalidations队列中
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        // 发送消息
        writePacket(qp, true);
    }     
    
    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    // 将数据pp发送给leader
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    // 从leader读取packet
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            // 将数据读取到pp中
            leaderIs.readRecord(pp, "packet");
        }
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    // 封装请求数据包到leader
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos
                .toByteArray(), request.authInfo);
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    // 查找leader节点
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        // 获取当前的选票,然后根据选票中记录的sid来获取leader
        Vote current = self.getCurrentVote();
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                // 重新初始化集群对象QuorumPeer中的addr和electionAddr
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return leaderServer;
    }
    
    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * 5 times before giving up. 
     * @param addr - the address of the Leader to connect to.
     * @throws IOException <li>if the socket connection fails on the 5th attempt</li>
     * <li>if there is an authentication failure while connecting to leader</li>
     * @throws ConnectException
     * @throws InterruptedException
     */
    // 与leader节点建立连接
    // 1.创建socket客户端
    // 2.初始化输入输出流
    protected void connectToLeader(InetSocketAddress addr, String hostname)
            throws IOException, ConnectException, InterruptedException {
        sock = new Socket();        
        sock.setSoTimeout(self.tickTime * self.initLimit);
        // 建立连接，最多重试5次
        for (int tries = 0; tries < 5; tries++) {
            try {
                sock.connect(addr, self.tickTime * self.syncLimit);
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                if (tries == 4) {
                    LOG.error("Unexpected exception",e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries="+tries+
                            ", connecting to " + addr,e);
                    sock = new Socket();
                    sock.setSoTimeout(self.tickTime * self.initLimit);
                }
            }
            Thread.sleep(1000);
        }
        // 身份认证
        self.authLearner.authenticate(sock, hostname);
        // 封装输入、输出流
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(
                sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
    }   
    
    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    // 以某种角色注册到leader上
    // 返回leader的zxid
    protected long registerWithLeader(int pktType) throws IOException{
        /*
         * Send follower info, including last zxid and sid
         */
        // 封装QuorumPacket
        // 设置当前zkServer的最新epoch
    	long lastLoggedZxid = self.getLastLoggedZxid();
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType);// 设置类型为Leader.FOLLOWERINFO或者Leader.OBSERVERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));// 设置自己的zxid
        
        /*
         * Add sid to payload
         */
        // 设置当前zkServer的sid
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000);
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        // 将自己的信息封装到数据包
        boa.writeRecord(li, "LearnerInfo");
        qp.setData(bsid.toByteArray());
        // 发送qp数据包
        writePacket(qp, true);
        // 读取数据到qp,leader此时发送的数据包内容如下:QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null)
        readPacket(qp);
        // 解析leader节点的epoch
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
        // 响应数据包类型为LEADERINFO,这里结合org.apache.zookeeper.server.quorum.LearnerHandler.run()方法分析
		if (qp.getType() == Leader.LEADERINFO) {
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
            // 读取leader的版本,写死的0x10000
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	// leader的epoch > 自己的acceptedEpoch
        	if (newEpoch > self.getAcceptedEpoch()) {
        	    // 设置自己currentEpoch到数据包准备返回给leader
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		// 更新自己的acceptedEpoch为leader的epoch
        		self.setAcceptedEpoch(newEpoch);
        	// 	leader的epoch = 自己的acceptedEpoch
        	} else if (newEpoch == self.getAcceptedEpoch()) {
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1);
        	} else {
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	// 封装ACKEPOCH数据包返回给leader
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
        	writePacket(ackNewEpoch, true);
        	// 更新自己的zxid中高32位的epoch为新的epoch,低32为0
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {
		    // 旧版本leader用于兼容
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    } 
    
    /**
     * Finally, synchronize our history with the Leader. 
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    // 同步learner和leader之间的数据
    // 1.前面registerWithLeader函数learner会回复leader的LEADERINFO,带上了自己的lastLoggedZxid
    // 2.leader根据lastLoggedZxid告诉learner是哪一种同步方式 DIFF同步,还是SNAP同步,还是先TRUNC回滚到某个zxid
    // 3.确定同步方式之后，leader会接着给learner发送后续的同步packet，分为
    //  PROPOSAL（提议）
    //  COMMIT（提交，针对Follower)
    //  INFORM（通知，针对Observer)
    //  UPTODATE(表示过半机器已完成同步，可以对外工作)
    //  NEWLEADER(leader告诉learner同步的相关请求已经发完了)
    protected void syncWithLeader(long newLeaderZxid) throws IOException, InterruptedException{
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        boolean snapshotNeeded = true;
        readPacket(qp);
        // 记录需要commit的zxid
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        // 记录收到的proposal但是还未commit
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>();
        synchronized (zk) {
            //  DIFF数据包(DIFF数据包显示集群中两个节点的 lastZxid 相同)
            if (qp.getType() == Leader.DIFF) {
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                snapshotNeeded = false;
            }
            // SNAP则从leader复制一份镜像数据到本地
            else if (qp.getType() == Leader.SNAP) {
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // clear our own database and read
                // 清空自己的数据库然后将leader节点数据保存到本地
                zk.getZKDatabase().clear();
                zk.getZKDatabase().deserializeSnapshot(leaderIs);
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            // TRUNC表示将本地数据回滚到leader节点返回的zxid的位置
            } else if (qp.getType() == Leader.TRUNC) {
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }
                // 更新本地内存数据库的lastProcessedZxid为leader发送过来的zxid
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            }
            else {
                LOG.error("Got unexpected packet from leader "
                        + qp.getType() + " exiting ... " );
                System.exit(13);

            }
            // 创建sesionTracker,来跟踪处理session会话
            zk.createSessionTracker();
            
            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) {// 启动时数据同步,不断读取leader的数据，直到收到UPTODATE表示同步完成
                // 读数据
                readPacket(qp);
                switch(qp.getType()) {
                // 收到一个提议,将提案记录到packetsNotCommitted中
                case Leader.PROPOSAL:
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid();
                    packetsNotCommitted.add(pif);
                    break;
                // 收到一个提交
                case Leader.COMMIT:
                    // 不需要快照，直接从packetsNotCommitted中获取一个未提交的数据，然后处理，最后移除
                    if (!writeToTxnLog) {
                        pif = packetsNotCommitted.peekFirst();
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);
                            packetsNotCommitted.remove();
                        }
                    // 需要快照处理，将zxid记录到packetsCommitted
                    } else {
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                // observer才会拿到INFORM消息，来同步
                case Leader.INFORM:
                    /*
                     * Only observer get this type of packet. We treat this
                     * as receiving PROPOSAL and COMMMIT.
                     */
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();
                    packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                    // Log warning message if txn comes out-of-order
                    if (packet.hdr.getZxid() != lastQueued + 1) {
                        LOG.warn("Got zxid 0x"
                                + Long.toHexString(packet.hdr.getZxid())
                                + " expected 0x"
                                + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = packet.hdr.getZxid();
                    if (!writeToTxnLog) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                    // 提案已经处理结束,退出循环,说明之前残留的投票已经处理完, 则将内存中的数据写入文件, 并发送 ACK 包
                case Leader.UPTODATE:
                    if (isPreZAB1_0) {
                        zk.takeSnapshot();
                        self.setCurrentEpoch(newEpoch);
                    }
                    // 注意这里此时才更新cnxnFactory中的zkServer为LearnerZooKeeperServer
                    self.cnxnFactory.setZooKeeperServer(zk);                
                    break outerLoop;
                    // NEWLEADER请求,
                case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery 
                    // means this is Zab 1.0
                    // Create updatingEpoch file and remove it after current
                    // epoch is set. QuorumPeer.loadDataBase() uses this file to
                    // detect the case where the server was terminated after
                    // taking a snapshot but before setting the current epoch.
                    File updating = new File(self.getTxnFactory().getSnapDir(),
                                        QuorumPeer.UPDATING_EPOCH_FILENAME);
                    if (!updating.exists() && !updating.createNewFile()) {
                        throw new IOException("Failed to create " +
                                              updating.toString());
                    }
                    if (snapshotNeeded) {
                        zk.takeSnapshot();
                    }
                    self.setCurrentEpoch(newEpoch);
                    if (!updating.delete()) {
                        throw new IOException("Failed to delete " +
                                              updating.toString());
                    }
                    writeToTxnLog = true; //Anything after this needs to go to the transaction log, not applied directly in memory
                    isPreZAB1_0 = false;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true);
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        // 启动zk服务器
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        // 更新当前选票
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.txn = p.rec;
                request.hdr = p.hdr;
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }
    // 在validateSession()方法中发送出REVALIDATE消息后接受到leader返回的响应
    protected void revalidate(QuorumPacket qp) throws IOException {
        // 读取响应
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        // 从pendingRevalidations队列中移除该ServerCnxn
        ServerCnxn cnxn = pendingRevalidations
        .remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            // 完成session的初始化
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }

    // learner接收leader的ping命令时，返回LearnerSessionTracker的快照
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        HashMap<Long, Integer> touchTable = zk
                .getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        // set the zookeeper server to null
        self.cnxnFactory.setZooKeeperServer(null);
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }
}
