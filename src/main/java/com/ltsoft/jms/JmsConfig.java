package com.ltsoft.jms;

import java.time.Duration;

/**
 * 运行配置信息
 */
public class JmsConfig {

    private Duration backDuration = Duration.ofMinutes(10);

    private int dupsCount;

    private Duration consumerExpire = Duration.ofHours(1);

    private Duration listenerKeepLive = Duration.ofMinutes(5);

    public Duration getBackDuration() {
        return backDuration;
    }

    public void setBackDuration(Duration backDuration) {
        this.backDuration = backDuration;
    }

    public int getDupsCount() {
        return dupsCount;
    }

    public void setDupsCount(int dupsCount) {
        this.dupsCount = dupsCount;
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
