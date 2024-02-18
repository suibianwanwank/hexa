package program.logical;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import program.util.PlannerSetting;

public class LogicalHepPlanner extends HepPlanner {
    private String name;

    private final PlannerSetting plannerSetting;

    public LogicalHepPlanner(HepProgram program, PlannerSetting plannerSetting) {
        super(program);
        this.plannerSetting = plannerSetting;
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
                    String.format("hep planner has be canceled, cancel type:%s", plannerSetting.getCancelReason()));
        }
    }
}
