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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 负责与zk服务端进行数据的发送
public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    private final Selector selector = Selector.open();
    // 在cleanup()方法中会被置为null
    // 默认初始化时为null
    // 初始化连接后会被赋值,参考registerAndConnect()方法
    private SelectionKey sockKey;

    ClientCnxnSocketNIO() throws IOException {
        super();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }
    
    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    // 处理读或写事件
    void doIO(List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
      throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        // 如果是OP_READ事件
        if (sockKey.isReadable()) {
            // 首先读取数据的长度
            int rc = sock.read(incomingBuffer);
            // 如果这个if判断成立,说明出现了连接关闭
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            // 判断incomingBuffer是否还有剩余空间,如果有说明出现了拆包,所以暂不处理,等待后续数据的到达
            if (!incomingBuffer.hasRemaining()) {
                // 执行到这里说明数据已全部读取完毕,开始处理
                // 转换一下,准备读取数据
                incomingBuffer.flip();
                // 如果条件成立,说明读取的是长度
                if (incomingBuffer == lenBuffer) {
                    recvCount++;
                    // 读取数据包的长度并根据长度,重新生成incomingBuffer
                    readLength();
                // 如果客户端和zkServer直接还没有初始化
                } else if (!initialized) {
                    // 读取ConnectRequest对应的服务端发送过来的响应ConnectResponse
                    readConnectResult();
                    // 增加OP_READ事件监听
                    enableRead();
                    // 判断是否有要发送的数据包
                    if (findSendablePacket(outgoingQueue,
                            cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        // 如果有,就增加OP_WRITE事件
                        enableWrite();
                    }
                    // 清空lenBuffer并将incomingBuffer重置为lenBuffer
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    // 初始化完毕
                    initialized = true;
                } else {
                    // 读取zkServer的响应
                    sendThread.readResponse(incomingBuffer);
                    // 清空lenBuffer并将incomingBuffer重置为lenBuffer
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        // 如果是OP_WRITE事件
        if (sockKey.isWritable()) {
            synchronized(outgoingQueue) {
                // 1.查找可发送的数据包
                Packet p = findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress());

                if (p != null) {
                    updateLastSend();
                    // If we already started writing p, p.bb will already exist
                    if (p.bb == null) {
                        // 如果请求头不为null,不是ConnectRequest数据包
                        // 如果请求头不为ping,不是ping数据包
                        // 如果请求头不为auth,不是auth数据包
                        if ((p.requestHeader != null) &&
                                (p.requestHeader.getType() != OpCode.ping) &&
                                (p.requestHeader.getType() != OpCode.auth)) {
                            // 设置请求头中的xid
                            p.requestHeader.setXid(cnxn.getXid());
                        }
                        // 组装数据到p.bb中
                        p.createBB();
                    }
                    // 将数据写入socket
                    sock.write(p.bb);
                    // 处理发送数据时的拆包问题
                    if (!p.bb.hasRemaining()) {
                        // 走到这里说明数据发送完毕
                        // 发送数+1
                        sentCount++;
                        // 从outgoingQueue中取出来，放到pendingQueue中
                        outgoingQueue.removeFirstOccurrence(p);
                        // 如果请求头不为null,不是ConnectRequest数据包
                        // 如果请求头不为ping,不是ping数据包
                        // 如果请求头不为auth,不是auth数据包
                        if (p.requestHeader != null
                                && p.requestHeader.getType() != OpCode.ping
                                && p.requestHeader.getType() != OpCode.auth) {
                            // 将以发送的数据包添加到pendingQueue队列中
                            synchronized (pendingQueue) {
                                pendingQueue.add(p);
                            }
                        }
                    }
                }
                // 要发送的数据包存储队列为空,取消OP_WRITE事件
                if (outgoingQueue.isEmpty()) {
                    // No more packets to send: turn off write interest flag.
                    // Will be turned on later by a later call to enableWrite(),
                    // from within ZooKeeperSaslClient (if client is configured
                    // to attempt SASL authentication), or in either doIO() or
                    // in doTransport() if not.
                    disableWrite();

                } else if (!initialized && p != null && !p.bb.hasRemaining()) {
                    // On initial connection, write the complete connect request
                    // packet, but then disable further writes until after
                    // receiving a successful connection response.  If the
                    // session is expired, then the server sends the expiration
                    // response and immediately closes its end of the socket.  If
                    // the client is simultaneously writing on its end, then the
                    // TCP stack may choose to abort with RST, in which case the
                    // client would never receive the session expired event.  See
                    // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                    disableWrite();
                } else {
                    // Just in case
                    enableWrite();
                }
            }
        }
    }

    /**
     * 因为ConnectRequest需要发送到SASL authentication进行处理，其他Packet都需要等待直到该处理完成，
     * ConnectRequest必须第一个处理，所以找出它并且把它放到OutgoingQueue头,也就是requestheader=null的那个
     * 初始化ConnectRequest:new ConnectRequest(0, lastZxid, sessionTimeout, sessId, sessionPasswd)
     * 初始化对应的Packet:new Packet(null, null, conReq,null, null, readOnly)
     * @param outgoingQueue
     * @param clientTunneledAuthenticationInProgress
     * @return
     */
    private Packet findSendablePacket(LinkedList<Packet> outgoingQueue,
                                      boolean clientTunneledAuthenticationInProgress) {
        synchronized (outgoingQueue) {
            if (outgoingQueue.isEmpty()) {
                return null;
            }
            if (outgoingQueue.getFirst().bb != null // If we've already starting sending the first packet, we better finish
                || !clientTunneledAuthenticationInProgress) {
                return outgoingQueue.getFirst();
            }

            // Since client's authentication with server is in progress,
            // send only the null-header packet queued by primeConnection().
            // This packet must be sent so that the SASL authentication process
            // can proceed, but all other packets should wait until
            // SASL authentication completes.
            ListIterator<Packet> iter = outgoingQueue.listIterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                if (p.requestHeader == null) {
                    // We've found the priming-packet. Move it to the beginning of the queue.
                    iter.remove();
                    outgoingQueue.add(0, p);
                    return p;
                } else {
                    // Non-priming packet: defer it until later, leaving it in the queue
                    // until authentication completes.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("deferring non-priming packet: " + p +
                                "until SASL authentication completes.");
                    }
                }
            }
            // no sendable packet found.
            return null;
        }
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }
 
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    // 创建一个Socket客户端
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    // 注册socket,如果是立即完成那么调用sendThread.primeConnection()
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) 
    throws IOException {
        // 1.注册OP_CONNECT事件,并初始化sockKey
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        // 2. 发起连接
        boolean immediateConnect = sock.connect(addr);
        // 3. 如果是立即建立成功,那么重新注册一些时间,然后关注OP_READ和OP_WRITE事件
        if (immediateConnect) {
            sendThread.primeConnection();
        }
    }

    // 已指定的参数地址建立sock连接
    @Override
    void connect(InetSocketAddress addr) throws IOException {
        // 1.初始化socket
        SocketChannel sock = createSock();
        try {
            // 2.与指定的地址建立连接
           registerAndConnect(sock, addr);
        } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getRemoteSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Returns the local address to which the socket is bound.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getLocalSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    // 唤醒阻塞的bio
    @Override
    synchronized void wakeupCnxn() {
        selector.wakeup();
    }

    // 客户端进行数据的发送以及接收
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue,
                     ClientCnxn cnxn)
            throws IOException, InterruptedException {
        // 获取就绪的SelectionKey
        selector.select(waitTimeOut);
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        // 发送数据前更新一下当前时间戳
        updateNow();
        // 遍历就绪的SelectionKey
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            // 如果是OP_CONNECT事件,该事件出现在org.apache.zookeeper.ClientCnxnSocketNIO.registerAndConnect()方法没有立即连接成功的情况下
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                // 判断是否完成连接
                if (sc.finishConnect()) {
                    // 更新一下最后发送数据和心跳的时间戳
                    updateLastSendAndHeard();
                    // 把watches和authData等数据发过去，并更新SelectionKey为读写
                    // 注:内部会创建ConnectRequest
                    sendThread.primeConnection();
                }
            // 如果是OP_READ或OP_WRITE事件
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                // 处理io事件
                doIO(pendingQueue, outgoingQueue, cnxn);
            }
        }
        if (sendThread.getZkState().isConnected()) {
            synchronized(outgoingQueue) {
                if (findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
                    enableWrite();
                }
            }
        }
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        ((SocketChannel) sockKey.channel()).socket().close();
    }

    @Override
    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    @Override
    public synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    synchronized void enableReadWriteOnly() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }


}
