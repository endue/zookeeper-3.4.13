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

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 */
// 监听器管理器
public class WatchManager {
    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);
    // key是路径,value是Watcher集合
    private final HashMap<String, HashSet<Watcher>> watchTable =
        new HashMap<String, HashSet<Watcher>>();
    // key是Watcher,value是路径集合
    private final HashMap<Watcher, HashSet<String>> watch2Paths =
        new HashMap<Watcher, HashSet<String>>();

    public synchronized int size(){
        int result = 0;
        for(Set<Watcher> watches : watchTable.values()) {
            result += watches.size();
        }
        return result;
    }

    /**
     * 添加路径的Watcher
     * @param path 路径
     * @param watcher 事件
     */
    public synchronized void addWatch(String path, Watcher watcher) {
        // 1.获取路径下的Watcher集合
        HashSet<Watcher> list = watchTable.get(path);
        if (list == null) {
            // don't waste memory if there are few watches on a node
            // rehash when the 4th entry is added, doubling size thereafter
            // seems like a good compromise
            list = new HashSet<Watcher>(4);
            watchTable.put(path, list);
        }
        // 1-1.添加Watcher到集合
        list.add(watcher);
        // 2.获取Watcher下的路径集合
        HashSet<String> paths = watch2Paths.get(watcher);
        if (paths == null) {
            // cnxns typically have many watches, so use default cap here
            paths = new HashSet<String>();
            watch2Paths.put(watcher, paths);
        }
        // 2-1.添加路径到集合
        paths.add(path);
    }

    /**
     * 删除Watcher
     * @param watcher
     */
    public synchronized void removeWatcher(Watcher watcher) {
        // 1.删除Watcher同时获取Watcher对应的路径
        HashSet<String> paths = watch2Paths.remove(watcher);
        if (paths == null) {
            return;
        }
        // 2.删除路径对应的Watcher
        for (String p : paths) {
            HashSet<Watcher> list = watchTable.get(p);
            if (list != null) {
                list.remove(watcher);
                if (list.size() == 0) {
                    watchTable.remove(p);
                }
            }
        }
    }

    /**
     * 触发Watcher
     * @param path
     * @param type
     * @return
     */
    public Set<Watcher> triggerWatch(String path, EventType type) {
        return triggerWatch(path, type, null);
    }

    /**
     * 触发Watcher
     * @param path
     * @param type
     * @param supress
     * @return
     */
    public Set<Watcher> triggerWatch(String path, EventType type, Set<Watcher> supress) {
        // 创建要通知的Watcher事件
        WatchedEvent e = new WatchedEvent(type,
                KeeperState.SyncConnected, path);
        // 记录所有要执行的Watcher
        HashSet<Watcher> watchers;
        synchronized (this) {
            watchers = watchTable.remove(path);
            if (watchers == null || watchers.isEmpty()) {
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG,
                            ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                            "No watchers for " + path);
                }
                return null;
            }
            for (Watcher w : watchers) {
                HashSet<String> paths = watch2Paths.get(w);
                if (paths != null) {
                    paths.remove(path);
                }
            }
        }
        // 遍历所有的Watcher,跳过参数supress中包含的Watcher
        // 执行Watcher的回调方法
        for (Watcher w : watchers) {
            if (supress != null && supress.contains(w)) {
                continue;
            }
            w.process(e);
        }
        // 返回要执行的Watcher事件
        return watchers;
    }

    /**
     * Brief description of this object.
     */
    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(watch2Paths.size()).append(" connections watching ")
            .append(watchTable.size()).append(" paths\n");

        int total = 0;
        for (HashSet<String> paths : watch2Paths.values()) {
            total += paths.size();
        }
        sb.append("Total watches:").append(total);

        return sb.toString();
    }

    /**
     * String representation of watches. Warning, may be large!
     * @param byPath iff true output watches by paths, otw output
     * watches by connection
     * @return string representation of watches
     */
    /**
     * dump出Watcher
     * @param pwriter
     * @param byPath
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        if (byPath) {
            for (Entry<String, HashSet<Watcher>> e : watchTable.entrySet()) {
                pwriter.println(e.getKey());
                for (Watcher w : e.getValue()) {
                    pwriter.print("\t0x");
                    pwriter.print(Long.toHexString(((ServerCnxn)w).getSessionId()));
                    pwriter.print("\n");
                }
            }
        } else {
            for (Entry<Watcher, HashSet<String>> e : watch2Paths.entrySet()) {
                pwriter.print("0x");
                pwriter.println(Long.toHexString(((ServerCnxn)e.getKey()).getSessionId()));
                for (String path : e.getValue()) {
                    pwriter.print("\t");
                    pwriter.println(path);
                }
            }
        }
    }
}
