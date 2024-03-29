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

package org.apache.zookeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.zookeeper.common.PathUtils;

/**
 * A parser for ZooKeeper Client connect strings.
 * 
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 * 
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 * 
 * @see org.apache.zookeeper.ZooKeeper
 */
// 处理ZooKeeper客户端连接字符串的解析器
public final class ConnectStringParser {
    // 默认端口号,客户端不指定端口是使用该断开
    private static final int DEFAULT_PORT = 2181;
    // 客户端所有操作的根路径
    private final String chrootPath;
    // 配置的所有zk集群服务地址
    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    /**
     * 解析connectString中记录的所有的zk服务ip+port
     * @throws IllegalArgumentException
     *             for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        // parse out chroot, if any
        // 获取客户端服务列表第一个/的位置,如127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a/
        // 例子中返回的off为44
        int off = connectString.indexOf('/');
        if (off >= 0) {
            // 然后截取字符串,例子中返回的是/app/a/
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            // 去掉用户操作的根路径,例子中返回的是127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
            connectString = connectString.substring(0, off);
        } else {
            this.chrootPath = null;
        }
        // 都会分割127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
        String hostsList[] = connectString.split(",");
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            int pidx = host.lastIndexOf(':');
            // 解析端口
            if (pidx >= 0) {
                // otherwise : is at the end of the string, ignore
                if (pidx < host.length() - 1) {
                    port = Integer.parseInt(host.substring(pidx + 1));
                }
                host = host.substring(0, pidx);
            }
            // 生成InetSocketAddress
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }
}