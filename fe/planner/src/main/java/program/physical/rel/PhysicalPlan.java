package program.physical.rel;

public interface PhysicalPlan {
    proto.datafusion.PhysicalPlanNode transformToDataFusionNode();
}
