package program.rule;

import context.QueryContext;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Permutation;
import program.program.RuleOptimizeProgram;
import program.util.RexUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rel.rules.ProjectMergeRule.DEFAULT_BLOAT;

public class RenameProjectRule implements RuleOptimizeProgram {

    public static final RenameProjectRule INSTANCE = new RenameProjectRule();

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        RelNode originRelNode = optimizeContext.getOriginRelNode();

        EnumerableProject renameProjectRel = createRenameProjectRel(root, originRelNode.getRowType());

        if(root instanceof EnumerableProject
                && RexUtils.containIdentity(renameProjectRel.getProjects(), renameProjectRel.getRowType(), renameProjectRel.getInput().getRowType(), String::compareTo)) {
            return root;
        }

        if (renameProjectRel.getInput() instanceof EnumerableProject) {
            RelNode mergeProj = projectMerge(renameProjectRel, (Project) renameProjectRel.getInput());
            return mergeProj == null ? renameProjectRel : mergeProj;
        }

        return renameProjectRel;
    }

    private static EnumerableProject createRenameProjectRel(RelNode rel, RelDataType rowType)
    {
        RelDataType t = rel.getRowType();

        RexBuilder b = rel.getCluster().getRexBuilder();
        List<RexNode> projections = new ArrayList<>();
        int projectCount = t.getFieldList().size();

        for (int i = 0; i < projectCount; i++) {
            projections.add(b.makeInputRef(rel, i));
        }

        boolean caseSensitive = rel.getCluster().getTypeFactory().getTypeSystem().isSchemaCaseSensitive();
        final List<String> fieldNames2 = SqlValidatorUtil.uniquify(rowType.getFieldNames(), SqlValidatorUtil.F_SUGGESTER, caseSensitive);

        RelDataType newRowType = RexUtil.createStructType(rel.getCluster().getTypeFactory(), projections, fieldNames2);
        return EnumerableProject.create(rel, projections, newRowType);
    }

    public @Nullable RelNode projectMerge(Project topProject, Project bottomProject)
    {
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(topProject.getCluster(), null);

        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if (topPermutation != null) {
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if (bottomPermutation != null) {
                final Permutation product = topPermutation.product(bottomPermutation);
                relBuilder.push(bottomProject.getInput());
                relBuilder.project(relBuilder.fields(product),
                        topProject.getRowType().getFieldNames());
                return relBuilder.build();
            }
        }

        final List<RexNode> newProjects =
                RelOptUtil.pushPastProjectUnlessBloat(topProject.getProjects(),
                        bottomProject, DEFAULT_BLOAT);
        if (newProjects == null) {
            // Merged projects are significantly more complex. Do not merge.
            return null;
        }
        final RelNode input = bottomProject.getInput();
        if (RexUtil.isIdentity(newProjects, input.getRowType())) {
            return input;
        }

        // replace the two projects with a combined projection
        relBuilder.push(bottomProject.getInput());
        relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
        return relBuilder.build();
    }
}
