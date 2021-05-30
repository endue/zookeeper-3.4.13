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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.quorum.ProposalStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 */
// 服务端针对某个客户端的ip+port，会保存一个对应的NIOServerCnxn
public class NIOServerCnxn extends ServerCnxn {
    static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);
    //
    NIOServerCnxnFactory factory;
    // 客户端的SocketChannel
    final SocketChannel sock;
    // 触发事件的sk
    protected final SelectionKey sk;
    // 是否初始化与客户端的连接,默认false
    boolean initialized;
    // 报文长度buffer
    ByteBuffer lenBuffer = ByteBuffer.allocate(4);
    // 初始化读取报文数据的buffer
    ByteBuffer incomingBuffer = lenBuffer;
    // 记录待发送的数据
    LinkedBlockingQueue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();

    int sessionTimeout;

    protected final ZooKeeperServer zkServer;

    /**
     * The number of requests that have been submitted but not yet responded to.
     * 记录已提交当时还未收到回复的请求,当处理请求超过阈值后,则关闭客户端的SelectionKey.OP_READ事件
     * 递增调用{@link org.apache.zookeeper.server.ZooKeeperServer#processPacket(ServerCnxn, ByteBuffer)}
     */
    int outstandingRequests;

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     */
    // 客户端的sessionId
    long sessionId;

    static long nextSessionId = 1;
    // 允许 的outstandingRequests的大小,超过该值后,取消对OP_READ的监听
    int outstandingLimit = 1;

    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
            SelectionKey sk, NIOServerCnxnFactory factory) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) { 
            outstandingLimit = zk.getGlobalOutstandingLimit();
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not
         * block */
        sock.socket().setSoLinger(false, -1);
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        // 注意这里默认的authInfo
        authInfo.add(new Id("ip", addr.getHostAddress()));
        // 监听OP_READ事件
        sk.interestOps(SelectionKey.OP_READ);
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in 
            * a tight while loop
            */
           sock.configureBlocking(true);
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.write(bb);
               }
               packetSent();
           } 
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }
    
    public void sendBuffer(ByteBuffer bb) {
        try {
            internalSendBuffer(bb);
        } catch(Exception e) {
            LOG.error("Unexpected Exception: ", e);
        }
    }

    /**
     * This method implements the internals of sendBuffer. We
     * have separated it from send buffer to be able to catch
     * exceptions when testing.
     *
     * @param bb Buffer to send.
     */
    protected void internalSendBuffer(ByteBuffer bb) {
        if (bb != ServerCnxnFactory.closeConn) {
            // We check if write interest here because if it is NOT set,
            // nothing is queued, so we can try to send the buffer right
            // away without waking up the selector
            if(sk.isValid() &&
                    ((sk.interestOps() & SelectionKey.OP_WRITE) == 0)) {
                try {
                    sock.write(bb);
                } catch (IOException e) {
                    // we are just doing best effort right now
                }
            }
            // if there is nothing left to send, we are done
            // bb发送完毕,则统计一下然后直接返回
            if (bb.remaining() == 0) {
                packetSent();
                return;
            }
        }
        // 走到这里说明参数bb发生了拆包,那么将bb暂存到outgoingBuffers队列中
        // 等待后续doIO()方法的处理
        synchronized(this.factory){
            sk.selector().wakeup();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                        + " is valid: " + sk.isValid());
            }
            outgoingBuffers.add(bb);
            if (sk.isValid()) {
                sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
            }
        }
    }

    /** Read the request payload (everything following the length prefix) */
    // 数据已经全部读取完毕,准备处理
    private void readPayload() throws IOException, InterruptedException {
        // 从SocketChannel读取数据到incomingBuffer
        // 有可能是第一次读取，也有可能是拆包后，处理后续发送过来的数据
        if (incomingBuffer.remaining() != 0) { // have we read length bytes?
            int rc = sock.read(incomingBuffer); // sock is non-blocking, so ok
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from client sessionid 0x"
                        + Long.toHexString(sessionId)
                        + ", likely client has closed socket");
            }
        }
        // 数据读取完毕
        // 清空incomingBuffer和lenBuffer
        // 将incomingBuffer重置为lenBuffer
        if (incomingBuffer.remaining() == 0) { // have we read length bytes?
            // 更新统计相关
            packetReceived();
            incomingBuffer.flip();
            if (!initialized) {
                // 处理连接请求并修改initialized为true
                readConnectRequest();
            } else {
                // 处理其他请求
                readRequest();
            }
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * Only used in order to allow testing
     */
    // 当前sock是否开启
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    @Override
    public InetAddress getSocketAddress() {
        if (sock == null) {
            return null;
        }

        return sock.socket().getInetAddress();
    }

    /**
     * Handles read/write IO on connection.
     */
    // 处理读或写事件
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            // 校验客户端是否开启
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x"
                         + Long.toHexString(sessionId));

                return;
            }
            // 处理读事件
            if (k.isReadable()) {
                // 初始化当前类时incomingBuffer == lenBuffer 4个字节长度
                // 读取数据到incomingBuffer
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException(
                            "Unable to read additional data from client sessionid 0x"
                            + Long.toHexString(sessionId)
                            + ", likely client has closed socket");
                }
                // 1.incomingBuffer.remaining() != 0 表示发生了拆包,不会处理此次请求中的数据会继续等待后续的数据
                // 2.incomingBuffer.remaining() == 0 处理请求，因为incomingBuffer读满了，也就是此次请求的数据全部读取到了或者此次请求的数据长度已经读取到了
                //  2.1 incomingBuffer == lenBuffer 表示是一次新的数据请求，此时incomingBuffer中存储的是数据的长度
                //  2.2 incomingBuffer != lenBuffer 表示不是新的一次数据请求，在上次读取数据的发生了拆包，所以这次继续读取incomingBuffer中的数据
                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    // 一次新的请求
                    if (incomingBuffer == lenBuffer) { // start of next request
                        // 读取这次新的请求中数据的长度，并重新初始化incomingBuffer
                        incomingBuffer.flip();
                        isPayload = readLength(k);
                        incomingBuffer.clear();
                    } else {
                        // continuation
                        // 继续处理上次拆包后的数据
                        isPayload = true;
                    }
                    // 如果isPayload为true表示不是4字节命令
                    if (isPayload) { // not the case for 4letterword
                        // 读取数据
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            // 处理写事件
            if (k.isWritable()) {
                // ZooLog.logTraceMessage(LOG,
                // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK
                // "outgoingBuffers.size() = " +
                // outgoingBuffers.size());
                // 判断outgoingBuffers是否有待发送的数据
                if (outgoingBuffers.size() > 0) {
                    // ZooLog.logTraceMessage(LOG,
                    // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK,
                    // "sk " + k + " is valid: " +
                    // k.isValid());

                    /*
                     * This is going to reset the buffer position to 0 and the
                     * limit to the size of the buffer, so that we can fill it
                     * with data from the non-direct buffers that we need to
                     * send.
                     */
                    // 获取直接内存缓冲区并清空,用来存储待发送的数据
                    ByteBuffer directBuffer = factory.directBuffer;
                    directBuffer.clear();
                    // 1.遍历outgoingBuffers中待处理的数据将数据放入directBuffer,直到directBuffer没有空间
                    for (ByteBuffer b : outgoingBuffers) {
                        // 如果directBuffer中剩余空间不足以放置当前b中的数据
                        if (directBuffer.remaining() < b.remaining()) {
                            /*
                             * When we call put later, if the directBuffer is to
                             * small to hold everything, nothing will be copied,
                             * so we've got to slice the buffer if it's too big.
                             */
                            // 将b拷贝一份,假设叫b'然后设置b'的limit为当前directBuffer剩余空间的大小
                            // 拷贝出来的b'和b各自的position和limit不相互受影响
                            // 最后将b'赋值给b,注意后续操作都是针对拷贝出来的b'进行的
                            b = (ByteBuffer) b.slice().limit(
                                    directBuffer.remaining());
                        }
                        /*
                         * put() is going to modify the positions of both
                         * buffers, put we don't want to change the position of
                         * the source buffers (we'll do that after the send, if
                         * needed), so we save and reset the position after the
                         * copy
                         */
                        // 获取b当前的position
                        int p = b.position();
                        // 将b中的数据赋值给directBuffer,内部会更新b的position
                        directBuffer.put(b);
                        // 还原b的position(这里很重要,对下面清空outgoingBuffers有辅助作用)
                        b.position(p);
                        // 没有剩余空间了，跳出当前循环
                        if (directBuffer.remaining() == 0) {
                            break;
                        }
                    }
                    /*
                     * Do the flip: limit becomes position, position gets set to
                     * 0. This sets us up for the write.
                     */
                    // 2.准备写数据到socket

                    // 翻转directBuffer
                    directBuffer.flip();
                    // 将directBuffer中的数据写入socket
                    // 这里返回了已发送的字节数
                    int sent = sock.write(directBuffer);
                    ByteBuffer bb;

                    // Remove the buffers that we have sent
                    // 3.数据已经写入socket,清空directBuffer中已发送的数据
                    while (outgoingBuffers.size() > 0) {
                        // 非阻塞,没有就返回null,注意这里只是peek并没有出队列
                        bb = outgoingBuffers.peek();
                        if (bb == ServerCnxnFactory.closeConn) {
                            throw new CloseRequestException("close requested");
                        }
                        // 首先拿出outgoingBuffers队列中的一个ByteBuffer
                        // 如果该ByteBuffer剩余数据 >  被发送的数据(这个sent下面会递减) 说明出现了拆包
                        // 当前ByteBuffer只发送了部分数据,更新ByteBuffer的position到未发生数据的初始位置
                        int left = bb.remaining() - sent;
                        if (left > 0) {
                            /*
                             * We only partially sent this buffer, so we update
                             * the position and exit the loop.
                             */
                            // 更新ByteBuffer的position到未发生数据的初始位置
                            // 退出while循环,此时的bb是不能从outgoingBuffers队列中剔除的
                            bb.position(bb.position() + sent);
                            break;
                        }
                        // 统计相关
                        packetSent();
                        /* We've sent the whole buffer, so drop the buffer */
                        // 由于bb在将数据写入directBuffer后还原了position,所以这里是可以递减的
                        sent -= bb.remaining();
                        // 从outgoingBuffers队列中剔除这个已经被完成发送出去的ByteBuffer
                        outgoingBuffers.remove();
                    }
                    // ZooLog.logTraceMessage(LOG,
                    // ZooLog.CLIENT_DATA_PACKET_TRACE_MASK, "after send,
                    // outgoingBuffers.size() = " + outgoingBuffers.size());
                }

                synchronized(this.factory){
                    // 记录到发送数据的队列已为空,取消OP_WRITE事件
                    if (outgoingBuffers.size() == 0) {
                        if (!initialized
                                && (sk.interestOps() & SelectionKey.OP_READ) == 0) {
                            throw new CloseRequestException("responded to info probe");
                        }
                        sk.interestOps(sk.interestOps()
                                & (~SelectionKey.OP_WRITE));
                    // 如果不为空,继续监听OP_WRITE事件
                    } else {
                        sk.interestOps(sk.interestOps()
                                | SelectionKey.OP_WRITE);
                    }
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x"
                     + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("EndOfStreamException stack trace", e);
            }
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x"
                     + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }
    // 将收到的数据交给ZooKeeperServer
    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }

    // 增加接收到的事务请求数(此时还未处理)
    // 并限制当前NIOServerCnxn中的SelectionKey是否继续监听OP_READ事件
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            synchronized (this) {
                outstandingRequests++;
            }
            synchronized (this.factory) {        
                // check throttling
                // 提交的request已超过阈值,停止OP_READ事件的处理
                if (zkServer.getInProcess() > outstandingLimit) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Throttling recv " + zkServer.getInProcess());
                    }
                    disableRecv();
                    // following lines should not be needed since we are
                    // already reading
                    // } else {
                    // enableRecv();
                }
            }
        }

    }

    // 取消OP_READ事件，也就是不在读取客户端的请求数据
    public void disableRecv() {
        sk.interestOps(sk.interestOps() & (~SelectionKey.OP_READ));
    }

    // 增加OP_READ事件，也就是开启读取客户端的请求数据
    public void enableRecv() {
        synchronized (this.factory) {
            sk.selector().wakeup();
            if (sk.isValid()) {
                int interest = sk.interestOps();
                if ((interest & SelectionKey.OP_READ) == 0) {
                    sk.interestOps(interest | SelectionKey.OP_READ);
                }
            }
        }
    }

    // 初始化连接请求
    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        // 初始化session
        zkServer.processConnectRequest(this, incomingBuffer);
        // 更新初始化标识符
        initialized = true;
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     * 
     * @param pwriter
     *            the pwriter for a command socket
     */
    private void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();
        
        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            checkFlush(true);
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }

    private static final String ZK_NOT_SERVING =
        "This ZooKeeper instance is not currently serving requests";
    
    /**
     * Set of threads for commmand ports. All the 4
     * letter commands are run via a thread. Each class
     * maps to a corresponding 4 letter command. CommandThread
     * is the abstract class from which all the others inherit.
     */
    private abstract class CommandThread extends Thread {
        PrintWriter pw;
        
        CommandThread(PrintWriter pw) {
            this.pw = pw;
        }
        
        public void run() {
            try {
                commandRun();
            } catch (IOException ie) {
                LOG.error("Error in running command ", ie);
            } finally {
                cleanupWriterSocket(pw);
            }
        }
        
        public abstract void commandRun() throws IOException;
    }
    
    private class RuokCommand extends CommandThread {
        public RuokCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            pw.print("imok");
            
        }
    }
    
    private class TraceMaskCommand extends CommandThread {
        TraceMaskCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            long traceMask = ZooTrace.getTextTraceLevel();
            pw.print(traceMask);
        }
    }
    
    private class SetTraceMaskCommand extends CommandThread {
        long trace = 0;
        SetTraceMaskCommand(PrintWriter pw, long trace) {
            super(pw);
            this.trace = trace;
        }
        
        @Override
        public void commandRun() {
            pw.print(trace);
        }
    }
    
    private class EnvCommand extends CommandThread {
        EnvCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            List<Environment.Entry> env = Environment.list();

            pw.println("Environment:");
            for(Environment.Entry e : env) {
                pw.print(e.getKey());
                pw.print("=");
                pw.println(e.getValue());
            }
            
        } 
    }
    
    private class ConfCommand extends CommandThread {
        ConfCommand(PrintWriter pw) {
            super(pw);
        }
            
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                zkServer.dumpConf(pw);
            }
        }
    }
    
    private class StatResetCommand extends CommandThread {
        public StatResetCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else {
                ServerStats serverStats = zkServer.serverStats();
                serverStats.reset();
                if (serverStats.getServerState().equals("leader")) {
                    ((LeaderZooKeeperServer)zkServer).getLeader().getProposalStats().reset();
                }
                pw.println("Server stats reset.");
            }
        }
    }
    
    private class CnxnStatResetCommand extends CommandThread {
        public CnxnStatResetCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                synchronized(factory.cnxns){
                    for(ServerCnxn c : factory.cnxns){
                        c.resetStats();
                    }
                }
                pw.println("Connection stats reset.");
            }
        }
    }

    private class DumpCommand extends CommandThread {
        public DumpCommand(PrintWriter pw) {
            super(pw);
        }
        
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else {
                pw.println("SessionTracker dump:");
                zkServer.sessionTracker.dumpSessions(pw);
                pw.println("ephemeral nodes dump:");
                zkServer.dumpEphemerals(pw);
            }
        }
    }
    
    private class StatCommand extends CommandThread {
        int len;
        public StatCommand(PrintWriter pw, int len) {
            super(pw);
            this.len = len;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            }
            else {   
                pw.print("Zookeeper version: ");
                pw.println(Version.getFullVersion());
                if (zkServer instanceof ReadOnlyZooKeeperServer) {
                    pw.println("READ-ONLY mode; serving only " +
                               "read-only clients");
                }
                if (len == statCmd) {
                    LOG.info("Stat command output");
                    pw.println("Clients:");
                    // clone should be faster than iteration
                    // ie give up the cnxns lock faster
                    HashSet<NIOServerCnxn> cnxnset;
                    synchronized(factory.cnxns){
                        cnxnset = (HashSet<NIOServerCnxn>)factory
                        .cnxns.clone();
                    }
                    for(NIOServerCnxn c : cnxnset){
                        c.dumpConnectionInfo(pw, true);
                        pw.println();
                    }
                    pw.println();
                }
                ServerStats serverStats = zkServer.serverStats();
                pw.print(serverStats.toString());
                pw.print("Node count: ");
                pw.println(zkServer.getZKDatabase().getNodeCount());
                if (serverStats.getServerState().equals("leader")) {
                    Leader leader = ((LeaderZooKeeperServer)zkServer).getLeader();
                    ProposalStats proposalStats = leader.getProposalStats();
                    pw.printf("Proposal sizes last/min/max: %s%n", proposalStats.toString());
                }
            }
            
        }
    }
    
    private class ConsCommand extends CommandThread {
        public ConsCommand(PrintWriter pw) {
            super(pw);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                // clone should be faster than iteration
                // ie give up the cnxns lock faster
                HashSet<NIOServerCnxn> cnxns;
                synchronized (factory.cnxns) {
                    cnxns = (HashSet<NIOServerCnxn>) factory.cnxns.clone();
                }
                for (NIOServerCnxn c : cnxns) {
                    c.dumpConnectionInfo(pw, false);
                    pw.println();
                }
                pw.println();
            }
        }
    }
    
    private class WatchCommand extends CommandThread {
        int len = 0;
        public WatchCommand(PrintWriter pw, int len) {
            super(pw);
            this.len = len;
        }

        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
            } else {
                DataTree dt = zkServer.getZKDatabase().getDataTree();
                if (len == wchsCmd) {
                    dt.dumpWatchesSummary(pw);
                } else if (len == wchpCmd) {
                    dt.dumpWatches(pw, true);
                } else {
                    dt.dumpWatches(pw, false);
                }
                pw.println();
            }
        }
    }

    private class MonitorCommand extends CommandThread {

        MonitorCommand(PrintWriter pw) {
            super(pw);
        }

        @Override
        public void commandRun() {
            if(!isZKServerRunning()) {
                pw.println(ZK_NOT_SERVING);
                return;
            }
            ZKDatabase zkdb = zkServer.getZKDatabase();
            ServerStats stats = zkServer.serverStats();

            print("version", Version.getFullVersion());

            print("avg_latency", stats.getAvgLatency());
            print("max_latency", stats.getMaxLatency());
            print("min_latency", stats.getMinLatency());

            print("packets_received", stats.getPacketsReceived());
            print("packets_sent", stats.getPacketsSent());
            print("num_alive_connections", stats.getNumAliveClientConnections());

            print("outstanding_requests", stats.getOutstandingRequests());

            print("server_state", stats.getServerState());
            print("znode_count", zkdb.getNodeCount());

            print("watch_count", zkdb.getDataTree().getWatchCount());
            print("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
            print("approximate_data_size", zkdb.getDataTree().approximateDataSize());

            OSMXBean osMbean = new OSMXBean();
            if (osMbean != null && osMbean.getUnix() == true) {
                print("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
                print("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());
            }

            print("fsync_threshold_exceed_count", stats.getFsyncThresholdExceedCount());

            if(stats.getServerState().equals("leader")) {
                Leader leader = ((LeaderZooKeeperServer)zkServer).getLeader();

                print("followers", leader.getLearners().size());
                print("synced_followers", leader.getForwardingFollowers().size());
                print("pending_syncs", leader.getNumPendingSyncs());

                print("last_proposal_size", leader.getProposalStats().getLastProposalSize());
                print("max_proposal_size", leader.getProposalStats().getMaxProposalSize());
                print("min_proposal_size", leader.getProposalStats().getMinProposalSize());
            }
        }

        private void print(String key, long number) {
            print(key, "" + number);
        }

        private void print(String key, String value) {
            pw.print("zk_");
            pw.print(key);
            pw.print("\t");
            pw.println(value);
        }

    }

    private class IsroCommand extends CommandThread {

        public IsroCommand(PrintWriter pw) {
            super(pw);
        }

        @Override
        public void commandRun() {
            if (!isZKServerRunning()) {
                pw.print("null");
            } else if (zkServer instanceof ReadOnlyZooKeeperServer) {
                pw.print("ro");
            } else {
                pw.print("rw");
            }
        }
    }

    private class NopCommand extends CommandThread {
        private String msg;

        public NopCommand(PrintWriter pw, String msg) {
            super(pw);
            this.msg = msg;
        }

        @Override
        public void commandRun() {
            pw.println(msg);
        }
    }

    /** Return if four letter word found and responded to, otw false **/
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
    throws IOException
    {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!ServerCnxn.isKnown(len)) {
            return false;
        }

        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open. 
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        String cmd = ServerCnxn.getCommandString(len);
        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!ServerCnxn.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, cmd + " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == ruokCmd) {
            RuokCommand ruok = new RuokCommand(pwriter);
            ruok.start();
            return true;
        } else if (len == getTraceMaskCmd) {
            TraceMaskCommand tmask = new TraceMaskCommand(pwriter);
            tmask.start();
            return true;
        } else if (len == setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }

            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, traceMask);
            setMask.start();
            return true;
        } else if (len == enviCmd) {
            EnvCommand env = new EnvCommand(pwriter);
            env.start();
            return true;
        } else if (len == confCmd) {
            ConfCommand ccmd = new ConfCommand(pwriter);
            ccmd.start();
            return true;
        } else if (len == srstCmd) {
            StatResetCommand strst = new StatResetCommand(pwriter);
            strst.start();
            return true;
        } else if (len == crstCmd) {
            CnxnStatResetCommand crst = new CnxnStatResetCommand(pwriter);
            crst.start();
            return true;
        } else if (len == dumpCmd) {
            DumpCommand dump = new DumpCommand(pwriter);
            dump.start();
            return true;
        } else if (len == statCmd || len == srvrCmd) {
            StatCommand stat = new StatCommand(pwriter, len);
            stat.start();
            return true;
        } else if (len == consCmd) {
            ConsCommand cons = new ConsCommand(pwriter);
            cons.start();
            return true;
        } else if (len == wchpCmd || len == wchcCmd || len == wchsCmd) {
            WatchCommand wcmd = new WatchCommand(pwriter, len);
            wcmd.start();
            return true;
        } else if (len == mntrCmd) {
            MonitorCommand mntr = new MonitorCommand(pwriter);
            mntr.start();
            return true;
        } else if (len == isroCmd) {
            IsroCommand isro = new IsroCommand(pwriter);
            isro.start();
            return true;
        }
        return false;
    }

    /** Reads the first 4 bytes of lenBuffer, which could be true length or
     *  four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    // 读取数据的总长度，为incomingBuffer重新分配空间
    // 同时还判断是否为4字节命令
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);
        return true;
    }

    public long getOutstandingRequests() {
        synchronized (this) {
            synchronized (this.factory) {
                return outstandingRequests;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public String toString() {
        return "NIOServerCnxn object with sock = " + sock + " and sk = " + sk;
    }

    /*
     * Close the cnxn and remove it from the factory cnxns list.
     * 
     * This function returns immediately if the cnxn is not on the cnxns list.
     */
    @Override
    public void close() {
        // 删除NIOServerCnxnFactory中记录的关于当前客户端的session和NIOServerCnxn
        factory.removeCnxn(this);
        // 删除当前客户端的事件
        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }
        // 关闭socket
        closeSock();
        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }
    }

    /**
     * Close resources associated with the sock of this cnxn. 
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.info("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                        " which had sessionid 0x" + Long.toHexString(sessionId) :
                        " (no session established for client)"));
        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
            // XXX The next line doesn't seem to be needed, but some posts
            // to forums suggest that it is needed. Keep in mind if errors in
            // this section arise.
            // factory.selector.wakeup();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }
    
    private final static byte fourBytes[] = new byte[4];

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#sendResponse(org.apache.zookeeper.proto.ReplyHeader,
     *      org.apache.jute.Record, java.lang.String)
     */
    // 发送响应
    @Override
    synchronized public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Make space for length
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            try {
                baos.write(fourBytes);
                // 设置响应头
                bos.writeRecord(h, "header");
                if (r != null) {
                    // 设置响应体
                    bos.writeRecord(r, tag);
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("Error serializing response");
            }
            byte b[] = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.putInt(b.length - 4).rewind();
            sendBuffer(bb);
            if (h.getXid() > 0) {
                // 服务端针对请求返回了响应,记录数递减1
                // 参考org.apache.zookeeper.server.FinalRequestProcessor.processRequest
                synchronized(this){
                    outstandingRequests--;
                }
                // check throttling
                synchronized (this.factory) {
                    // 判断是否订阅对应客户端的OP_READ事件
                    if (zkServer.getInProcess() < outstandingLimit
                            || outstandingRequests < 1) {
                        sk.selector().wakeup();
                        enableRecv();
                    }
                }
            }
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     * 触发事件
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     * {@link org.apache.zookeeper.server.WatchManager.triggerWatch}
     */
    @Override
    synchronized public void process(WatchedEvent event) {
        // 设置请求头
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }
        // 封装事件响应并发送
        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();

        sendResponse(h, e, "notification");
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    // 1.设置sessionId到当前的serverCnxn
    // 2.设置当前的serverCnxn到serverCnxnFactory的sessionMap中
    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        this.factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public int getInterestOps() {
        return sk.isValid() ? sk.interestOps() : 0;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (!isZKServerRunning()) {
            return null;
        }
        return zkServer.serverStats();
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }
}
