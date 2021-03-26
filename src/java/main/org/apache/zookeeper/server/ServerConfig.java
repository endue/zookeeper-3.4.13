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

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * Server configuration storage.
 *
 * We use this instead of Properties as it's typed.
 *
 */
// 存储服务配置,单机模式启动时使用
@InterfaceAudience.Public
public class ServerConfig {
    ////
    //// If you update the configuration parameters be sure
    //// to update the "conf" 4letter word
    ////
    // 服务器的ip+port
    protected InetSocketAddress clientPortAddress;
    // 内存中的数据存储成快照文件snapshot的目录
    // 默认情况下，事务日志也会存储在这里。建议同时配置参数dataLogDir,事务日志的写性能直接影响zk性能
    protected String dataDir;
    // 事务日志输出目录
    protected String dataLogDir;
    // zookeeper基本时间单位,默认3000ms
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    // 客户端最大连接数
    protected int maxClientCnxns;
    /** defaults to -1 if not set explicitly */
    protected int minSessionTimeout = -1;
    /** defaults to -1 if not set explicitly */
    protected int maxSessionTimeout = -1;

    /**
     * Parse arguments for server configuration
     * @param args clientPort dataDir and optional tickTime and maxClientCnxns
     * @return ServerConfig configured wrt arguments
     * @throws IllegalArgumentException on invalid usage
     */
    // 通过代码可以看出,参数args长度要求[2,4]两边都是闭区间
    public void parse(String[] args) {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException("Invalid number of arguments:" + Arrays.toString(args));
        }
        // 数组第1个值为端口号
        clientPortAddress = new InetSocketAddress(Integer.parseInt(args[0]));
        // 数组第2个值为内存数据库快照路径
        dataDir = args[1];
        // 事务日志的路径
        dataLogDir = dataDir;
        if (args.length >= 3) {
            tickTime = Integer.parseInt(args[2]);
        }
        if (args.length == 4) {
            maxClientCnxns = Integer.parseInt(args[3]);
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @return ServerConfig configured wrt arguments
     * @throws ConfigException error processing configuration
     */
    // 根据路径读取配置文件
    public void parse(String path) throws ConfigException {
        QuorumPeerConfig config = new QuorumPeerConfig();
        // 解析配置文件
        config.parse(path);

        // let qpconfig parse the file and then pull the stuff we are
        // interested in
        readFrom(config);
    }

    /**
     * Read attributes from a QuorumPeerConfig.
     * @param config
     */
    // 读取配置文件
    public void readFrom(QuorumPeerConfig config) {
      clientPortAddress = config.getClientPortAddress();
      dataDir = config.getDataDir();
      dataLogDir = config.getDataLogDir();
      tickTime = config.getTickTime();
      maxClientCnxns = config.getMaxClientCnxns();
      minSessionTimeout = config.getMinSessionTimeout();
      maxSessionTimeout = config.getMaxSessionTimeout();
    }

    public InetSocketAddress getClientPortAddress() {
        return clientPortAddress;
    }
    public String getDataDir() { return dataDir; }
    public String getDataLogDir() { return dataLogDir; }
    public int getTickTime() { return tickTime; }
    public int getMaxClientCnxns() { return maxClientCnxns; }
    /** minimum session timeout in milliseconds, -1 if unset */
    public int getMinSessionTimeout() { return minSessionTimeout; }
    /** maximum session timeout in milliseconds, -1 if unset */
    public int getMaxSessionTimeout() { return maxSessionTimeout; }
}
