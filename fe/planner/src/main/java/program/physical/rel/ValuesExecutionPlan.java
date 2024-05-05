package program.physical.rel;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import proto.datafusion.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ValuesExecutionPlan extends Values
        implements EnumerableRel, ExecutionPlan {
    private ValuesExecutionPlan(RelOptCluster cluster, RelDataType rowType,
                                ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
        super(cluster, rowType, tuples, traitSet);
    }

    /**
     * Creates an EnumerableValues.
     */
    public static ValuesExecutionPlan create(RelOptCluster cluster,
                                             final RelDataType rowType,
                                             final ImmutableList<ImmutableList<RexLiteral>> tuples) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.values(mq, rowType, tuples))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.values(rowType, tuples));
        return new ValuesExecutionPlan(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new ValuesExecutionPlan(getCluster(), getRowType(), tuples, traitSet);
    }

    @Override
    public @Nullable RelNode passThrough(final RelTraitSet required) {
        RelCollation collation = required.getCollation();
        if (collation == null || collation.isDefault()) {
            return null;
        }

        // A Values with 0 or 1 rows can be ordered by any collation.
        if (tuples.size() > 1) {
            Ordering<List<RexLiteral>> ordering = null;
            // Generate ordering comparator according to the required collations.
            for (RelFieldCollation fc : collation.getFieldCollations()) {
                Ordering<List<RexLiteral>> comparator = RelMdCollation.comparator(fc);
                if (ordering == null) {
                    ordering = comparator;
                } else {
                    ordering = ordering.compound(comparator);
                }
            }
            // Check whether the tuples are sorted by required collations.
            if (!requireNonNull(ordering, "ordering").isOrdered(tuples)) {
                return null;
            }
        }

        // The tuples order satisfies the collation, we just create a new
        // relnode with required collation info.
        return copy(traitSet.replace(collation), ImmutableList.of());
    }

    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.PROHIBITED;
    }

    @Override
    public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref) {
/*
          return Linq4j.asEnumerable(
              new Object[][] {
                  new Object[] {1, 2},
                  new Object[] {3, 4}
              });
*/
        final JavaTypeFactory typeFactory =
                (JavaTypeFactory) getCluster().getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferCustom());
        final Type rowClass = physType.getJavaRowType();

        final List<Expression> expressions = new ArrayList<>();
        final List<RelDataTypeField> fields = getRowType().getFieldList();
        for (List<RexLiteral> tuple : tuples) {
            final List<Expression> literals = new ArrayList<>();
            for (Pair<RelDataTypeField, RexLiteral> pair
                    : Pair.zip(fields, tuple)) {
                literals.add(
                        RexToLixTranslator.translateLiteral(
                                pair.right,
                                pair.left.getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL));
            }
            expressions.add(physType.record(literals));
        }
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit(
                                        Primitive.box(rowClass), expressions))));
        return implementor.result(physType, builder.toBlock());
    }

    @Override
    public PhysicalPlanNode transformToDataFusionNode() {
        proto.datafusion.ProjectionExecNode.Builder builder = proto.datafusion.ProjectionExecNode.newBuilder();

        // TODO need fix
        if (tuples.size() > 1) {
            throw new CommonException(CommonErrorCode.PLAN_TRANSFORM_ERROR, "Not support value tuples size > 1");
        }
        for (RexLiteral rexLiteral : tuples.get(0)) {
            ScalarValue scalarValue = PhysicalPlanTransformUtil.transformLiteral(rexLiteral);
            builder.addExpr(proto.datafusion.PhysicalExprNode.newBuilder()
                    .setLiteral(scalarValue));
        }

        for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
            builder.addExprName(relDataTypeField.getName());
        }

        PlaceholderRowExecNode.Builder placeholderRowExecNode = PlaceholderRowExecNode.newBuilder()
                .setSchema(proto.datafusion.Schema.newBuilder());

        builder.setInput(proto.datafusion.PhysicalPlanNode.newBuilder()
                .setPlaceholderRow(placeholderRowExecNode));

        return proto.datafusion.PhysicalPlanNode.newBuilder()
                .setProjection(builder)
                .build();
    }
}
