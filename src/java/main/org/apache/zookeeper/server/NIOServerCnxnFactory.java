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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责处理客户端的相关连接等操作
 * 该类实现了Runnable接口并且包含ServerSocketChannel，所以可以肯定run()方法包含在一个循环的whlie中，来处理客户端的相关连接
 * 同时也会将当前类交给一个Thread来启动
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    static {
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch(IOException ie) {
            LOG.error("Selector failed to open", ie);
        }
    }
    // ServerSocketChannel
    // 监听配置文件中配置的clientPortAddress(默认不配置取当前服务器IP)和clientPort
    ServerSocketChannel ss;
    // 初始化Selector
    final Selector selector = Selector.open();

    /**
     * We use this buffer to do efficient socket I/O. Since there is a single
     * sender thread per NIOServerCnxn instance, we can use a member variable to
     * only allocate it once.
    */
    // 内存缓存区，写死64kb
    final ByteBuffer directBuffer = ByteBuffer.allocateDirect(64 * 1024);
    // 记录InetAddress(只包含IP地址,不包含端口号)对应的Set<NIOServerCnxn>
    final HashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new HashMap<InetAddress, Set<NIOServerCnxn>>( );
    // 单个客户端最大连接数
    int maxClientCnxns = 60;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     * @throws IOException
     */
    public NIOServerCnxnFactory() throws IOException {
    }
    // 由于当前NIOServerCnxnFactory实现了Runable接口，所以在configure()方法中，将当前类传入了该thread
    // 最后靠启动thread来执行NIOServerCnxnFactory类中的run()方法
    Thread thread;

    /**
     * 配置ServerSocketChannel并监听OP_ACCEPT事件
     * @param addr 当前服务器的InetSocketAddress对象
     * @param maxcc 客户端最大连接数
     * @throws IOException
     */
    @Override
    public void configure(InetSocketAddress addr, int maxcc) throws IOException {
        configureSaslLogin();
        // 创建thread并传入当前类，这里并没有启动thread
        thread = new ZooKeeperThread(this, "NIOServerCxn.Factory:" + addr);
        thread.setDaemon(true);
        // 设置客户端最大连接数
        maxClientCnxns = maxcc;
        // 初始化ServerSocketChannel，并监听OP_ACCEPT事件
        this.ss = ServerSocketChannel.open();
        // 为了确保一个进程被关闭后，即使它还没有释放该端口，
        // 同一个主机上的其他进程可以立刻重用该端口，可以调用Socket的setResuseAddress(true)
        // 注意在Socket还没有绑定到一个本地端口之前调用
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port " + addr);
        // 设置非阻塞并绑定断开
        ss.socket().bind(addr);
        ss.configureBlocking(false);
        // 注册OP_ACCEPT事件
        ss.register(selector, SelectionKey.OP_ACCEPT);
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    // 集群模式启动方法
    // 注:单机模式也会调用该方法
    @Override
    public void start() {
        // ensure thread is started once and only once
        if (thread.getState() == Thread.State.NEW) {
            // 启动在configure()方法中创建的线程，也就是启动了当前类
            thread.start();
        }
    }

    // 单机模式启动方法
    @Override
    public void startup(ZooKeeperServer zks) throws IOException,
            InterruptedException {
        // 启动当前类
        start();
        // zks就是ZooKeeperServer
        setZooKeeperServer(zks);
        // 初始化ZKDatabase
        zks.startdata();
        // 启动ZooKeeperServer
        zks.startup();
    }

    @Override
    public InetSocketAddress getLocalAddress(){
        return (InetSocketAddress)ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort(){
        return ss.socket().getLocalPort();
    }

    // 添加一个NIOServerCnxn
    private void addCnxn(NIOServerCnxn cnxn) {
        synchronized (cnxns) {
            cnxns.add(cnxn);
            synchronized (ipMap){
                InetAddress addr = cnxn.sock.socket().getInetAddress();
                Set<NIOServerCnxn> s = ipMap.get(addr);
                if (s == null) {
                    // in general we will see 1 connection from each
                    // host, setting the initial cap to 2 allows us
                    // to minimize mem usage in the common case
                    // of 1 entry --  we need to set the initial cap
                    // to 2 to avoid rehash when the first entry is added
                    s = new HashSet<NIOServerCnxn>(2);
                    s.add(cnxn);
                    ipMap.put(addr,s);
                } else {
                    s.add(cnxn);
                }
            }
        }
    }
    // 删除NIOServerCnxn
    public void removeCnxn(NIOServerCnxn cnxn) {
        synchronized(cnxns) {
            // Remove the related session from the sessionMap.
            long sessionId = cnxn.getSessionId();
            if (sessionId != 0) {
                sessionMap.remove(sessionId);
            }

            // if this is not in cnxns then it's already closed
            if (!cnxns.remove(cnxn)) {
                return;
            }

            synchronized (ipMap) {
                Set<NIOServerCnxn> s =
                        ipMap.get(cnxn.getSocketAddress());
                s.remove(cnxn);
            }

            unregisterConnection(cnxn);
        }
    }

    // 初始化一个NIOServerCnxn，并监听OP_READ事件
    protected NIOServerCnxn createConnection(SocketChannel sock,
            SelectionKey sk) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this);
    }
    // 获取InetAddress当前的连接数
    private int getClientCnxnCount(InetAddress cl) {
        // The ipMap lock covers both the map, and its contents
        // (that is, the cnxn sets shouldn't be modified outside of
        // this lock)
        synchronized (ipMap) {
            Set<NIOServerCnxn> s = ipMap.get(cl);
            if (s == null) return 0;
            return s.size();
        }
    }
    // 当该类被调用startup()或start()方法时会执行该方法
    // 不断while循环，处理客户端的请求
    public void run() {
        while (!ss.socket().isClosed()) {
            try {
                // 超时等待1000
                selector.select(1000);
                // 获取就绪的SelectionKey
                Set<SelectionKey> selected;
                synchronized (this) {
                    selected = selector.selectedKeys();
                }
                // 就绪key存储到list
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(
                        selected);
                // 打乱SelectionKey，随机排序
                // 防止出现总是先处理某个客户端的请求
                Collections.shuffle(selectedList);
                // 遍历处理就绪的key
                for (SelectionKey k : selectedList) {
                    // 客户端连接事件
                    if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel sc = ((ServerSocketChannel) k
                                .channel()).accept();
                        // 获取客户端的InetAddress对象
                        InetAddress ia = sc.socket().getInetAddress();
                        // 获取客户端当前的连接数
                        int cnxncount = getClientCnxnCount(ia);
                        // 超过单个客户端允许的最大连接
                        if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns){
                            LOG.warn("Too many connections from " + ia
                                     + " - max is " + maxClientCnxns );
                            // 关闭当前连接
                            sc.close();
                        // 未超过
                        } else {
                            LOG.info("Accepted socket connection from "
                                     + sc.socket().getRemoteSocketAddress());
                            // 监听OP_READ事件
                            sc.configureBlocking(false);
                            SelectionKey sk = sc.register(selector,
                                    SelectionKey.OP_READ);
                            // 初始化一个NIOServerCnxn
                            NIOServerCnxn cnxn = createConnection(sc, sk);
                            // 将NIOServerCnxn绑定到SelectionKey上
                            sk.attach(cnxn);
                            // 添加NIOServerCnxn
                            addCnxn(cnxn);
                        }
                    // 客户端读或写事件
                    } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        // 获取这个key之前绑定的NIOServerCnxn
                        NIOServerCnxn c = (NIOServerCnxn) k.attachment();
                        // 这里可以看出每个key的事件处理会交给它所对应的NIOServerCnxn
                        c.doIO(k);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Unexpected ops in select "
                                      + k.readyOps());
                        }
                    }
                }
                // 最后清空selected
                selected.clear();
            } catch (RuntimeException e) {
                LOG.warn("Ignoring unexpected runtime exception", e);
            } catch (Exception e) {
                LOG.warn("Ignoring exception", e);
            }
        }
        closeAll();
        LOG.info("NIOServerCnxn factory exited run method");
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    synchronized public void closeAll() {
        selector.wakeup();
        HashSet<NIOServerCnxn> cnxns;
        synchronized (this.cnxns) {
            cnxns = (HashSet<NIOServerCnxn>)this.cnxns.clone();
        }
        // got to clear all the connections that we have in the selector
        for (NIOServerCnxn cnxn: cnxns) {
            try {
                // don't hold this.cnxns lock as deadlock may occur
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                         + Long.toHexString(cnxn.sessionId), e);
            }
        }
    }

    public void shutdown() {
        try {
            ss.close();
            closeAll();
            thread.interrupt();
            thread.join();
            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            selector.close();
        } catch (IOException e) {
            LOG.warn("Selector closing", e);
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    // 关闭session
    @Override
    public synchronized void closeSession(long sessionId) {
        selector.wakeup();
        closeSessionWithoutWakeup(sessionId);
    }

    // 关闭session
    @SuppressWarnings("unchecked")
    private void closeSessionWithoutWakeup(long sessionId) {
        NIOServerCnxn cnxn = (NIOServerCnxn) sessionMap.remove(sessionId);
        if (cnxn != null) {
            try {
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("exception during session close", e);
            }
        }
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }
}
