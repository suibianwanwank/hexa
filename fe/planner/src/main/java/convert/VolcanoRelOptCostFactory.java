package convert;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class VolcanoRelOptCostFactory implements RelOptCostFactory {
    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
        return null;
    }

    @Override
    public RelOptCost makeHugeCost() {
        return null;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return null;
    }

    @Override
    public RelOptCost makeTinyCost() {
        return null;
    }

    @Override
    public RelOptCost makeZeroCost() {
        return null;
    }
}
