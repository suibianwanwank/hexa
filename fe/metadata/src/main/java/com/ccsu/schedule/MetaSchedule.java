package com.ccsu.schedule;

public class MetaSchedule {
    private MetaScheduleType type;

    private String clusterId;

    private String catalogName;

    private long cycleTime;

    private long fireTime;

    public MetaSchedule(MetaScheduleType type, String clusterId, String catalogName, long cycleTime, long fireTime) {
        this.type = type;
        this.clusterId = clusterId;
        this.catalogName = catalogName;
        this.cycleTime = cycleTime;
        this.fireTime = fireTime;
    }

    public MetaSchedule nextSchedule(long nextFireTime) {
        return new MetaSchedule(type, clusterId, catalogName, cycleTime, nextFireTime);
    }

    public MetaScheduleType getType() {
        return type;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public long getCycleTime() {
        return cycleTime;
    }

    @Override
    public String toString() {
        return "MetaSchedule{"
                + "type=" + type
                + ", clusterId='" + clusterId + '\''
                + ", catalogName='" + catalogName + '\''
                + ", cycleTime=" + cycleTime
                + '}';
    }
}
