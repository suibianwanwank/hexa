package program.util;

import org.apache.calcite.plan.Context;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PlannerSetting implements Context {

    private PlannerCancelFlag cancelFlag;

    public PlannerSetting(PlannerCancelFlag cancelFlag) {
        this.cancelFlag = cancelFlag;
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        if (aClass == PlannerSetting.class) {
            return aClass.cast(this);
        }
        throw null;
    }

    public boolean isCancelRequested() {
        return cancelFlag.isCancelRequested();
    }

    public String getCancelReason() {
        return cancelFlag.getCancelReason();
    }

    public void timeRestAndStart() {
        cancelFlag.timeRestAndStart();
    }

    public void timeStop() {
        cancelFlag.timeStop();
    }
}
