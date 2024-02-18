package handler;

import arrow.datafusion.protobuf.PhysicalPlanNode;
import com.ccsu.Result;

public class QueryPlanResult implements Result<PhysicalPlanNode> {
    private PhysicalPlanNode physicalPlan;
    private String errMsg;
    private boolean isSuccess;

    public QueryPlanResult() {
    }

    public static QueryPlanResult err(String msg) {
        QueryPlanResult result = new QueryPlanResult();
        result.isSuccess = false;
        result.errMsg = msg;
        return result;
    }

    public static QueryPlanResult success(PhysicalPlanNode node) {
        QueryPlanResult result = new QueryPlanResult();
        result.isSuccess = true;
        result.physicalPlan = node;
        return result;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public PhysicalPlanNode getResult() {
        return physicalPlan;
    }

    @Override
    public String getErrorMsg() {
        return errMsg;
    }
}
