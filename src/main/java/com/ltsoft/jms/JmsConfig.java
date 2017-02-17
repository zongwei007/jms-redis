package com.ltsoft.jms;

import java.time.Duration;

/**
 * 运行配置信息
 */
public class JmsConfig {

    private int dupsCount = 10;

    private int cacheThread;

    private int scheduledThread = Math.max(2, Runtime.getRuntime().availableProcessors());

    private Duration backDuration = Duration.ofMinutes(10);

    private Duration consumerExpire = Duration.ofHours(1);

    private Duration listenerKeepLive = Duration.ofMinutes(5);

    public int getDupsCount() {
        return dupsCount;
    }

    public void setDupsCount(int dupsCount) {
        this.dupsCount = Math.max(5, dupsCount);
    }

    public int getCacheThread() {
        return cacheThread;
    }

    public void setCacheThread(int cacheThread) {
        this.cacheThread = cacheThread;
    }

    public int getScheduledThread() {
        return scheduledThread;
    }

    public void setScheduledThread(int scheduledThread) {
        this.scheduledThread = Math.max(2, scheduledThread);
    }

    public Duration getBackDuration() {
        return backDuration;
    }

    public void setBackDuration(Duration backDuration) {
        this.backDuration = backDuration;
    }

    public Duration getConsumerExpire() {
        return consumerExpire;
    }

    public void setConsumerExpire(Duration consumerExpire) {
        this.consumerExpire = consumerExpire;
    }

    public Duration getListenerKeepLive() {
        return listenerKeepLive;
    }

    public void setListenerKeepLive(Duration listenerKeepLive) {
        this.listenerKeepLive = listenerKeepLive;
    }
}
