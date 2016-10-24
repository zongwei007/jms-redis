package com.ltsoft.jms.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 线程池工具
 */
public final class ThreadPool {

    private static final ExecutorService CACHED_POOL = Executors.newCachedThreadPool();

    private static final ScheduledExecutorService SCHEDULED_POOL = Executors.newScheduledThreadPool(5);

    private ThreadPool() {
        //禁用构造函数
    }

    public static ExecutorService cachedPool() {
        return CACHED_POOL;
    }

    public static ScheduledExecutorService scheduledPool() {
        return SCHEDULED_POOL;
    }

    public static void shutdown() {
        CACHED_POOL.shutdown();
    }

}
