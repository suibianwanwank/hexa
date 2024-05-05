package program.physical.rel;

public interface ExecutionPlan {
    proto.datafusion.PhysicalPlanNode transformToDataFusionNode();
}
