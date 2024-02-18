package program.util;

import com.google.common.base.Stopwatch;
import org.apache.calcite.util.CancelFlag;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PlannerCancelFlag extends CancelFlag {

    private long timeoutThreshold;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private CancelType cancelType = CancelType.NOT_CANCEL;

    public static PlannerCancelFlag create(long timeoutThreshold) {
        PlannerCancelFlag cancelFlag = new PlannerCancelFlag(new AtomicBoolean(false));
        cancelFlag.timeoutThreshold = timeoutThreshold;
        return cancelFlag;
    }

    private PlannerCancelFlag(AtomicBoolean atomicBoolean) {
        super(atomicBoolean);
    }

    @Override
    public boolean isCancelRequested() {
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        if (timeoutThreshold > 0 && elapsed >= timeoutThreshold) {
            this.cancelType = CancelType.TIMEOUT;
            return true;
        }
        return super.isCancelRequested();
    }

    public String getCancelReason() {
        return cancelType.getCancelReason();
    }

    public void timeRestAndStart() {
        stopwatch.reset();
        stopwatch.start();
    }

    public void timeStop() {
        stopwatch.stop();
    }

    public enum CancelType {

        /**
         * origin status
         */
        NOT_CANCEL("Planner has not been cancelled."),
        /**
         * Planner is canceled by timeout
         */
        TIMEOUT("Planner timeout"),

        /**
         * Planner is canceled by client
         */
        CLIENT("Client canceled");

        private final String cancelReason;

        CancelType(String cancelReason) {
            this.cancelReason = cancelReason;
        }

        public String getCancelReason() {
            return cancelReason;
        }
    }
}
