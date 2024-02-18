package program.physical;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import program.util.PlannerSetting;

public class PhysicalVolcanoPlanner extends VolcanoPlanner {

    private final PlannerSetting plannerSetting;

    public PhysicalVolcanoPlanner(PlannerSetting plannerSetting) {
        this.plannerSetting = plannerSetting;
    }

    @Override
    public Context getContext() {
        return plannerSetting;
    }

    @Override
    public RelNode findBestExp() {
        plannerSetting.timeRestAndStart();
        RelNode bestExp;
        try {
            bestExp = super.findBestExp();
        } finally {
            plannerSetting.timeStop();
        }
        return bestExp;
    }

    @Override
    public void checkCancel() {
        PlannerSetting plannerSetting = getContext().unwrap(PlannerSetting.class);
        if (plannerSetting != null && plannerSetting.isCancelRequested()) {
            throw new CommonException(CommonErrorCode.PLANNER_CANCEL_ERROR,
                    String.format("volcano planner has be canceled, cancel type:%s", plannerSetting.getCancelReason()));
        }
    }
}
