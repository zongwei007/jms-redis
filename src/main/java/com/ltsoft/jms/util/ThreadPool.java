package com.ltsoft.jms.util;

import com.ltsoft.jms.JmsConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 线程池工具
 */
public final class ThreadPool {

    private final ExecutorService cachedPool;

    private final ScheduledExecutorService scheduledPool;

    public ThreadPool(JmsConfig jmsConfig) {
        if (jmsConfig.getCacheThread() > 0) {
            this.cachedPool = Executors.newFixedThreadPool(jmsConfig.getCacheThread());
        } else {
            this.cachedPool = Executors.newCachedThreadPool();
        }

        this.scheduledPool = Executors.newScheduledThreadPool(jmsConfig.getScheduledThread());
    }

    public ExecutorService cachedPool() {
        return cachedPool;
    }

    public ScheduledExecutorService scheduledPool() {
        return scheduledPool;
    }

    public void shutdown() {
        cachedPool.shutdown();
    }

}
