package program.logical;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.option.OptionManager;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import program.util.PlannerCancelFlag;
import program.util.PlannerSetting;

public class LogicalVolcanoPlanner extends VolcanoPlanner {

    private final PlannerSetting plannerSetting;


    public static LogicalVolcanoPlanner of(OptionManager optionManager) {
//        PlannerCancelFlag plannerCancelFlag =
//                PlannerCancelFlag.create(optionManager.getLongOption(OptionConstants.PLANNER_TIME_OUT_MS.name()));
        PlannerCancelFlag plannerCancelFlag = PlannerCancelFlag.create(-1L);
        PlannerSetting plannerSetting = new PlannerSetting(plannerCancelFlag);
        LogicalVolcanoPlanner planner = new LogicalVolcanoPlanner(plannerSetting);
        planner.clearRelTraitDefs();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        return new LogicalVolcanoPlanner(plannerSetting);
    }

    public LogicalVolcanoPlanner(PlannerSetting plannerSetting) {
        this.plannerSetting = plannerSetting;
        clearRelTraitDefs();
        addRelTraitDef(ConventionTraitDef.INSTANCE);
        // TODO: 2022/10/11 add DistributionTraitDef.INSTANCE
        addRelTraitDef(RelCollationTraitDef.INSTANCE);
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
