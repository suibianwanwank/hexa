package program.physical.rel;

import arrow.datafusion.protobuf.PhysicalPlanNode;

public interface PhysicalPlan {
    PhysicalPlanNode transformToDataFusionNode();
}
