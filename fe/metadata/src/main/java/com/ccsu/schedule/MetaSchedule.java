package com.ccsu.schedule;

public class MetaSchedule {
    private CollectInfo collectInfo;

    private long cycleTime;

    private long fireTime;

    public MetaSchedule(CollectInfo collectInfo, long cycleTime, long fireTime) {
        this.collectInfo = collectInfo;
        this.cycleTime = cycleTime;
        this.fireTime = fireTime;
    }

    public MetaSchedule nextSchedule(long nextFireTime) {
        return new MetaSchedule(collectInfo, cycleTime, nextFireTime);
    }

    public CollectInfo getCollectInfo() {
        return collectInfo;
    }

    public long getCycleTime() {
        return cycleTime;
    }

    @Override
    public String toString() {
        return "MetaSchedule{" +
                "collectInfo=" + collectInfo +
                ", cycleTime=" + cycleTime +
                ", fireTime=" + fireTime +
                '}';
    }
}
