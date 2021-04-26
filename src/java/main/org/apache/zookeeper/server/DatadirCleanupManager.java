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

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the cleanup of snapshots and corresponding transaction
 * logs by scheduling the auto purge task with the specified
 * 'autopurge.purgeInterval'. It keeps the most recent
 * 'autopurge.snapRetainCount' number of snapshots and corresponding transaction
 * logs.
 */
public class DatadirCleanupManager {

    private static final Logger LOG = LoggerFactory.getLogger(DatadirCleanupManager.class);

    /**
     * Status of the dataDir purge task
     * 启动状态枚举
     */
    public enum PurgeTaskStatus {
        NOT_STARTED, STARTED, COMPLETED;
    }
    // 记录启动状态
    private PurgeTaskStatus purgeTaskStatus = PurgeTaskStatus.NOT_STARTED;
    // 如果没有特别指明，ZooKeeper将内存中的数据和事务日志写入该位置
    private final String snapDir;
    // 如果配置选项则将事务日志写入dataLogDir而不是dataDir，从而将事物日志和数据快照分开
    private final String dataLogDir;
    // 快照保存的数量，默认3，最小也为3
    private final int snapRetainCount;
    // PurgeTask触发的时间间隔(单位小时)，默认0
    private final int purgeInterval;

    private Timer timer;

    /**
     * Constructor of DatadirCleanupManager. It takes the parameters to schedule
     * the purge task.
     * 
     * @param snapDir 用于存放内存数据快照的文件夹，同时用于集群的myid文件也存在这个文件夹里
     *            snapshot directory
     * @param dataLogDir 事务日志写入该配置指定的目录，而不是“ dataDir ”所指定的目录
     *            transaction log directory
     * @param snapRetainCount
     *            number of snapshots to be retained after purge
     * @param purgeInterval
     *            purge interval in hours
     */
    public DatadirCleanupManager(String snapDir, String dataLogDir, int snapRetainCount,
            int purgeInterval) {
        this.snapDir = snapDir;
        this.dataLogDir = dataLogDir;
        this.snapRetainCount = snapRetainCount;
        this.purgeInterval = purgeInterval;
        LOG.info("autopurge.snapRetainCount set to " + snapRetainCount);
        LOG.info("autopurge.purgeInterval set to " + purgeInterval);
    }

    /**
     * Validates the purge configuration and schedules the purge task. Purge
     * task keeps the most recent <code>snapRetainCount</code> number of
     * snapshots and deletes the remaining for every <code>purgeInterval</code>
     * hour(s).
     * <p>
     * <code>purgeInterval</code> of <code>0</code> or
     * <code>negative integer</code> will not schedule the purge task.
     * </p>
     * 
     * @see PurgeTxnLog#purge(File, File, int)
     */
    // 启动
    public void start() {
        // 校验启动状态
        if (PurgeTaskStatus.STARTED == purgeTaskStatus) {
            LOG.warn("Purge task is already running.");
            return;
        }
        // Don't schedule the purge task with zero or negative purge interval.
        // 校验执行间隔,必须大于0
        if (purgeInterval <= 0) {
            LOG.info("Purge task is not scheduled.");
            return;
        }
        // 创建定时清理任务并传入数据快照和事物日志的路径，然后启动并定期执行
        timer = new Timer("PurgeTask", true);
        TimerTask task = new PurgeTask(dataLogDir, snapDir, snapRetainCount);
        // 每purgeInterval小时执行一次
        timer.scheduleAtFixedRate(task, 0, TimeUnit.HOURS.toMillis(purgeInterval));
        // 标记为启动状态
        purgeTaskStatus = PurgeTaskStatus.STARTED;
    }

    /**
     * Shutdown the purge task.
     */
    public void shutdown() {
        if (PurgeTaskStatus.STARTED == purgeTaskStatus) {
            LOG.info("Shutting down purge task.");
            timer.cancel();
            purgeTaskStatus = PurgeTaskStatus.COMPLETED;
        } else {
            LOG.warn("Purge task not started. Ignoring shutdown!");
        }
    }
    // 清理数据快照和事物日志的任务
    static class PurgeTask extends TimerTask {
        // 事物日志路径
        private String logsDir;
        // 数据快照路径
        private String snapsDir;
        // 保存的数量
        private int snapRetainCount;

        public PurgeTask(String dataDir, String snapDir, int count) {
            logsDir = dataDir;
            snapsDir = snapDir;
            snapRetainCount = count;
        }

        @Override
        public void run() {
            LOG.info("Purge task started.");
            try {
                // 开始清理日志
                PurgeTxnLog.purge(new File(logsDir), new File(snapsDir), snapRetainCount);
            } catch (Exception e) {
                LOG.error("Error occurred while purging.", e);
            }
            LOG.info("Purge task completed.");
        }
    }

    /**
     * Returns the status of the purge task.
     * 
     * @return the status of the purge task
     */
    public PurgeTaskStatus getPurgeTaskStatus() {
        return purgeTaskStatus;
    }

    /**
     * Returns the snapshot directory.
     * 
     * @return the snapshot directory.
     */
    public String getSnapDir() {
        return snapDir;
    }

    /**
     * Returns transaction log directory.
     * 
     * @return the transaction log directory.
     */
    public String getDataLogDir() {
        return dataLogDir;
    }

    /**
     * Returns purge interval in hours.
     * 
     * @return the purge interval in hours.
     */
    public int getPurgeInterval() {
        return purgeInterval;
    }

    /**
     * Returns the number of snapshots to be retained after purge.
     * 
     * @return the number of snapshots to be retained after purge.
     */
    public int getSnapRetainCount() {
        return snapRetainCount;
    }
}
