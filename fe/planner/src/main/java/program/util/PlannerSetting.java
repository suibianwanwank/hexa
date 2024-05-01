package program.util;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import org.apache.calcite.plan.Context;
import org.apache.calcite.util.CancelFlag;
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
        if(aClass == CancelFlag.class){
            return aClass.cast(cancelFlag);
        }
        throw new CommonException(CommonErrorCode.PLANNER_CANCEL_ERROR, "sda");
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
