package com.ccsu.result;

import com.ccsu.Result;
import org.apache.calcite.rel.RelNode;

public class QueryPlanResult implements Result {
    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public RelNode getResult() {
        return null;
    }
}
