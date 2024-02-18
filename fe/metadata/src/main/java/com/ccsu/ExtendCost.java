package com.ccsu;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;

/**
 * Implementation of the cbo cost model.
 * It is the calcite {@link RelOptCost} implementation class,
 * has added the network and memory.
 */
public class ExtendCost implements RelOptCost {
    private static final double MEMORY_TO_CPU_RATIO = 1.0;

    private final double rowCount;
    private final double cpu;
    private final double io;
    private final double bandWidth;
    private final double ms;
    private final double memory;

    static final ExtendCost INFINITY = new ExtendCost(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY) {
        @Override
        public String toString() {
            return "{inf}";
        }
    };

    static final ExtendCost HUGE = new ExtendCost(
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE) {
        @Override
        public String toString() {
            return "{huge}";
        }
    };

    static final ExtendCost ZERO = new ExtendCost(
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0) {
        @Override
        public String toString() {
            return "{0}";
        }
    };

    static final ExtendCost TINY = new ExtendCost(
            1.0,
            1.0,
            0.0,
            0.0,
            0.0,
            0.0) {
        @Override
        public String toString() {
            return "{tiny}";
        }
    };

    public ExtendCost(double rowCount, double cpu, double io, double bandWidth, double ms, double memory) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
        this.bandWidth = bandWidth;
        this.ms = ms;
        this.memory = memory;
    }

    public double getBandWidth() {
        return this.bandWidth;
    }

    public double getMemory() {
        return this.memory;
    }

    @Override
    public double getRows() {
        return this.rowCount;
    }

    @Override
    public double getCpu() {
        return this.cpu;
    }

    @Override
    public double getIo() {
        return this.io;
    }

    @Override
    public boolean isInfinite() {
        return (this == INFINITY)
                || (this.cpu == Double.POSITIVE_INFINITY)
                || (this.io == Double.POSITIVE_INFINITY)
                || (this.bandWidth == Double.POSITIVE_INFINITY)
                || (this.rowCount == Double.POSITIVE_INFINITY)
                || (this.memory == Double.POSITIVE_INFINITY);
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof ExtendCost)) {
            return false;
        }

        ExtendCost that = (ExtendCost) other;
        return (this == that)
                || ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
                && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
                && (Math.abs(this.bandWidth - that.bandWidth) < RelOptUtil.EPSILON)
                && (Math.abs(this.ms - that.ms) < RelOptUtil.EPSILON)
                && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
                && (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON));
    }

    @Override
    public boolean isLe(RelOptCost other) {
        ExtendCost that = (ExtendCost) other;

        return this == that
                || this.equals(other)
                || this.isLt(other);
    }

    @Override
    public boolean isLt(RelOptCost other) {
        ExtendCost that = (ExtendCost) other;

        // Compare the difference of each cost factor one-by-one instead of aggregating them together
        // to minimize issues from double truncation.
        return (that.cpu - this.cpu)
                + (that.io - this.io)
                + (that.bandWidth - this.bandWidth)
                + (that.ms - this.ms)
                + (that.memory - this.memory) * ExtendCost.MEMORY_TO_CPU_RATIO > 0;
    }

    @Override
    public RelOptCost plus(RelOptCost other) {
        ExtendCost that = (ExtendCost) other;

        if (this == INFINITY || that == INFINITY) {
            return INFINITY;
        }

        return new ExtendCost(
                this.rowCount + that.rowCount,
                this.cpu + that.cpu,
                this.io + that.io,
                this.bandWidth + that.bandWidth,
                this.ms + that.ms,
                this.memory + that.memory);
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        }
        ExtendCost that = (ExtendCost) other;

        return new ExtendCost(
                this.rowCount - that.rowCount,
                this.cpu - that.cpu,
                this.io - that.io,
                this.bandWidth - that.bandWidth,
                this.ms - that.ms,
                this.memory - that.memory);
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) {
            return INFINITY;
        }

        return new ExtendCost(
                rowCount * factor,
                cpu * factor,
                io * factor,
                bandWidth * factor,
                ms * factor,
                memory * factor);
    }

    @Override
    public double divideBy(RelOptCost other) {
        ExtendCost that = (ExtendCost) other;

        double d = 1.0;
        double n = 0.0;

        if (this.rowCount != 0
                && !Double.isInfinite(this.rowCount)
                && (that.rowCount != 0) && !Double.isInfinite(that.rowCount)) {
            d *= this.rowCount / that.rowCount;
            n += 1;
        }

        if (this.cpu != 0
                && !Double.isInfinite(this.cpu)
                && (that.cpu != 0)
                && !Double.isInfinite(that.cpu)) {
            d *= this.cpu / that.cpu;
            n += 1;
        }

        if ((this.io != 0)
                && !Double.isInfinite(this.io)
                && (that.io != 0)
                && !Double.isInfinite(that.io)) {
            d *= this.io / that.io;
            n += 1;
        }

        if (this.bandWidth != 0
                && !Double.isInfinite(this.bandWidth)
                && that.bandWidth != 0
                && !Double.isInfinite(that.bandWidth)) {
            d *= this.bandWidth / that.bandWidth;
            n += 1;
        }

        if (this.ms != 0
                && !Double.isInfinite(this.ms)
                && that.ms != 0
                && !Double.isInfinite(that.ms)) {
            d *= this.ms / that.ms;
            n += 1;
        }

        if (this.memory != 0
                && !Double.isInfinite(this.memory)
                && that.memory != 0
                && !Double.isInfinite(that.memory)) {
            d *= this.memory / that.memory;
            n += 1;
        }

        if (n == 0) {
            return 1.0;
        }

        return Math.pow(d, 1 / n);
    }

    public Double evaluateOverallCost() {
        return rowCount + cpu + io + bandWidth + ms + memory;
    }

    @Override
    public boolean equals(RelOptCost other) {
        return this == other
                || (other instanceof ExtendCost
                && (this.cpu == ((ExtendCost) other).cpu)
                && (this.io == ((ExtendCost) other).io)
                && (this.bandWidth == ((ExtendCost) other).bandWidth)
                && (this.ms == ((ExtendCost) other).ms)
                && (this.rowCount == ((ExtendCost) other).rowCount)
                && (this.memory == ((ExtendCost) other).memory));
    }

    @Override
    public String toString() {
        return "{" + rowCount + " rows, "
                + cpu + " cpu, "
                + io + " io, "
                + bandWidth + " bandWidth, "
                + ms + " ms, "
                + memory + " memory}";
    }
}
