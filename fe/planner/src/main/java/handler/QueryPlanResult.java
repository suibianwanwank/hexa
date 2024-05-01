package handler;


import com.ccsu.Result;

public class QueryPlanResult implements Result<proto.datafusion.PhysicalPlanNode> {
    private proto.datafusion.PhysicalPlanNode physicalPlan;
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

    public static QueryPlanResult success(proto.datafusion.PhysicalPlanNode node) {
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
    public proto.datafusion.PhysicalPlanNode getResult() {
        return physicalPlan;
    }

    @Override
    public String getErrorMsg() {
        return errMsg;
    }
}
