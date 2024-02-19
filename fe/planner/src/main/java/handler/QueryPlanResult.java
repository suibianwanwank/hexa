package handler;

import arrow.datafusion.PhysicalPlanNode;
import com.ccsu.Result;

public class QueryPlanResult implements Result<PhysicalPlanNode> {
    private final PhysicalPlanNode physicalPlan;

    public QueryPlanResult(PhysicalPlanNode physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public PhysicalPlanNode getResult() {
        return physicalPlan;
    }
}
