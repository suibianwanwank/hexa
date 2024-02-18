package handler;

import arrow.datafusion.PhysicalPlan;
import com.ccsu.Result;

public class QueryPlanResult implements Result<PhysicalPlan> {
    private final PhysicalPlan physicalPlan;

    public QueryPlanResult(PhysicalPlan physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public PhysicalPlan getResult() {
        return physicalPlan;
    }
}
