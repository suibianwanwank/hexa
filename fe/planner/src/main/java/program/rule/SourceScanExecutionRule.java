package program.rule;

import com.ccsu.meta.CommonTranslateTable;
import com.ccsu.meta.data.TableInfo;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import program.physical.rel.SourceScanExecutionPlan;

/**
 * Planner rule that converts a {@link LogicalTableScan} to an {@link SourceScanExecutionRule}.
 * You may provide a custom config to convert other nodes that extend {@link TableScan}.
 *
 * @see org.apache.calcite.adapter.enumerable.EnumerableRules#ENUMERABLE_TABLE_SCAN_RULE
 */
public class SourceScanExecutionRule extends ConverterRule {

    /**
     * Default configuration.
     */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalTableScan.class,
                    r -> EnumerableTableScan.canHandle(r.getTable()),
                    Convention.NONE, EnumerableConvention.INSTANCE,
                    "EnumerableSqlScanRule")
            .withRuleFactory(SourceScanExecutionRule::new);

    public static final SourceScanExecutionRule ENUMERABLE_SQL_SCAN_RULE =
            SourceScanExecutionRule.DEFAULT_CONFIG.toRule(SourceScanExecutionRule.class);

    protected SourceScanExecutionRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        TableScan scan = (TableScan) rel;
        final RelOptTable relOptTable = scan.getTable();
        final Table table = relOptTable.unwrap(Table.class);
        // The QueryableTable can only be implemented as ENUMERABLE convention,
        // but some test QueryableTables do not really implement the expressions,
        // just skips the QueryableTable#getExpression invocation and returns early.
        if (table instanceof QueryableTable || relOptTable.getExpression(Object.class) != null) {
            return EnumerableTableScan.create(scan.getCluster(), relOptTable);
        }

        if (relOptTable instanceof Prepare.PreparingTable) {
            CommonTranslateTable physicalTable = (relOptTable).unwrap(CommonTranslateTable.class);
            if (physicalTable == null) {
                return null;
            }
            TableInfo metadataTable = physicalTable.getTableInfo();
            String schemaName = metadataTable.getSchemaName();
            String tableName = metadataTable.getTableName();
            return SourceScanExecutionPlan.create(physicalTable.getSourceConfig(),
                    String.format("select * from %s.%s", schemaName, tableName),
                    rel.getCluster(), relOptTable, Object.class);

        }

        return null;
    }
}
