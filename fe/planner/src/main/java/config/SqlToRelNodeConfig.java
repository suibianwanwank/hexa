package config;

import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.UnaryOperator;

public class SqlToRelNodeConfig implements SqlToRelConverter.Config {
    private boolean decorrelationEnabled;
    private boolean trimUnusedFields;
    private boolean createValuesRel;
    private boolean explain;
    private boolean expand;
    private int inSubQueryThreshold;
    private boolean removeSortInSubQuery;
    private RelBuilderFactory relBuilderFactory;
    private UnaryOperator<RelBuilder.Config> relBuilderConfigTransform;
    private HintStrategyTable hintStrategyTable;
    private boolean addJsonTypeOperatorEnabled;

    @Override
    public SqlToRelConverter.Config withDecorrelationEnabled(boolean decorrelationEnabled) {
        this.decorrelationEnabled = decorrelationEnabled;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withTrimUnusedFields(boolean trimUnusedFields) {
        this.trimUnusedFields = trimUnusedFields;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withCreateValuesRel(boolean createValuesRel) {
        this.createValuesRel = createValuesRel;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withExplain(boolean explain) {
        this.explain = explain;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withExpand(boolean expand) {
        this.expand = expand;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withInSubQueryThreshold(int threshold) {
        this.inSubQueryThreshold = threshold;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withRemoveSortInSubQuery(boolean removeSortInSubQuery) {
        this.removeSortInSubQuery = removeSortInSubQuery;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withRelBuilderFactory(RelBuilderFactory factory) {
        this.relBuilderFactory = factory;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withRelBuilderConfigTransform(UnaryOperator<RelBuilder.Config> transform) {
        this.relBuilderConfigTransform = transform;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withHintStrategyTable(HintStrategyTable hintStrategyTable) {
        this.hintStrategyTable = hintStrategyTable;
        return this;
    }

    @Override
    public SqlToRelConverter.Config withAddJsonTypeOperatorEnabled(boolean addJsonTypeOperatorEnabled) {
        this.addJsonTypeOperatorEnabled = addJsonTypeOperatorEnabled;
        return this;
    }

    @Override
    public SqlToRelConverter.Config addRelBuilderConfigTransform(UnaryOperator<RelBuilder.Config> transform) {
        this.relBuilderConfigTransform.andThen(transform);
        return this;
    }

    @Override
    public boolean isDecorrelationEnabled() {
        return this.decorrelationEnabled;
    }

    @Override
    public boolean isTrimUnusedFields() {
        return this.trimUnusedFields;
    }

    @Override
    public boolean isCreateValuesRel() {
        return this.createValuesRel;
    }

    @Override
    public boolean isExplain() {
        return this.explain;
    }

    @Override
    public boolean isExpand() {
        return this.expand;
    }

    @Override
    public int getInSubQueryThreshold() {
        return this.inSubQueryThreshold;
    }

    @Override
    public boolean isRemoveSortInSubQuery() {
        return this.removeSortInSubQuery;
    }

    @Override
    public RelBuilderFactory getRelBuilderFactory() {
        return this.relBuilderFactory;
    }

    @Override
    public UnaryOperator<RelBuilder.Config> getRelBuilderConfigTransform() {
        return this.relBuilderConfigTransform;
    }

    @Override
    public HintStrategyTable getHintStrategyTable() {
        return this.hintStrategyTable;
    }

    @Override
    public boolean isAddJsonTypeOperatorEnabled() {
        return this.addJsonTypeOperatorEnabled;
    }
}
