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
import java.util.LinkedList;
import java.util.List;

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ClientCnxnSocket does the lower level communication with a socket
 * implementation.
 * 
 * This code has been moved out of ClientCnxn so that a Netty implementation can
 * be provided as an alternative to the NIO socket code.
 * 
 */
abstract class ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocket.class);
    // client和zkServer连接初始化完成标识符
    protected boolean initialized;

    /**
     * This buffer is only used to read the length of the incoming message.
     */
    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);

    /**
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;
    // 记录从outgoingQueue队列中发送出去的数据包的数量
    protected long sentCount = 0;
    // 接收到的数据包数量
    protected long recvCount = 0;
    // 最后一次发送心跳的时间戳
    protected long lastHeard;
    // 最后一次发送消息的时间戳
    protected long lastSend;
    // 当前时间戳
    protected long now;
    // 设置SendThread,在Clientcnxn运行SendThread时,传入该值
    protected ClientCnxn.SendThread sendThread;

    /**
     * The sessionId is only available here for Log and Exception messages.
     * Otherwise the socket doesn't need to know it.
     */
    protected long sessionId;

    void introduce(ClientCnxn.SendThread sendThread, long sessionId) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
    }

    void updateNow() {
        now = Time.currentElapsedTime();
    }

    int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    /**
     * 距离上次发送数据已经过去的时间
     * 有可能一直接收数据导致now变更了但是lastSend没变更
     * @return
     */
    int getIdleSend() {
        return (int) (now - lastSend);
    }

    long getSentCount() {
        return sentCount;
    }

    long getRecvCount() {
        return recvCount;
    }

    void updateLastHeard() {
        this.lastHeard = now;
    }

    void updateLastSend() {
        this.lastSend = now;
    }

    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    // 读取数据包的长度并根据长度,重新生成incomingBuffer
    protected void readLength() throws IOException {
        // 读取长度
        int len = incomingBuffer.getInt();
        if (len < 0 || len >= ClientCnxn.packetLen) {
            throw new IOException("Packet len" + len + " is out of range!");
        }
        // 重新生成incomingBuffer
        incomingBuffer = ByteBuffer.allocate(len);
    }

    // 读取ConnectResponse
    void readConnectResult() throws IOException {
        if (LOG.isTraceEnabled()) {
            StringBuilder buf = new StringBuilder("0x[");
            for (byte b : incomingBuffer.array()) {
                buf.append(Integer.toHexString(b) + ",");
            }
            buf.append("]");
            LOG.trace("readConnectResult " + incomingBuffer.remaining() + " "
                    + buf.toString());
        }
        ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        ConnectResponse conRsp = new ConnectResponse();
        conRsp.deserialize(bbia, "connect");

        // read "is read-only" flag
        boolean isRO = false;
        try {
            isRO = bbia.readBool("readOnly");
        } catch (IOException e) {
            // this is ok -- just a packet from an old server which
            // doesn't contain readOnly field
            LOG.warn("Connected to an old server; r-o mode will be unavailable");
        }
        // 获取sessionId
        this.sessionId = conRsp.getSessionId();
        // 初始化响应后的连接
        sendThread.onConnected(conRsp.getTimeOut(), this.sessionId,
                conRsp.getPasswd(), isRO);
    }

    abstract boolean isConnected();

    abstract void connect(InetSocketAddress addr) throws IOException;

    abstract SocketAddress getRemoteSocketAddress();

    abstract SocketAddress getLocalSocketAddress();

    abstract void cleanup();

    abstract void close();

    abstract void wakeupCnxn();

    abstract void enableWrite();

    abstract void disableWrite();

    abstract void enableReadWriteOnly();

    abstract void doTransport(int waitTimeOut, List<Packet> pendingQueue,
            LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException;

    abstract void testableCloseSocket() throws IOException;

    abstract void sendPacket(Packet p) throws IOException;
}
