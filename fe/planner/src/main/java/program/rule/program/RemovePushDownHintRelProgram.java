package program.rule.program;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.pojo.DatasourceType;
import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import program.physical.dialect.RelToSqlConverter;
import program.physical.rel.PushDownHintRel;
import program.program.RuleOptimizeProgram;

public class RemovePushDownHintRelProgram implements RuleOptimizeProgram {

    public static final RemovePushDownHintRelProgram DEFAULT = new RemovePushDownHintRelProgram();

    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        return root.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof PushDownHintRel) {
                    PushDownHintRel hintRel = (PushDownHintRel) other;
                    SqlDialect sqlDialect = getSqlDialect(hintRel.getTableScan().getSourceType());

                    RelToSqlConverter sqlConverter = new RelToSqlConverter(sqlDialect);

                    String pushDownSql = sqlConverter.convertRelToSqlNode(hintRel.getInput()).
                            toSqlString(sqlDialect).getSql();

                    return hintRel.getTableScan().copy(pushDownSql, hintRel.getRowType());
                }
                return super.visit(other);
            }
        });
    }

    private SqlDialect getSqlDialect(DatasourceType datasourceType) {
        switch (datasourceType) {
            case MYSQL: {
                return MysqlSqlDialect.DEFAULT;
            }
            case POSTGRESQL: {
                return PostgresqlSqlDialect.DEFAULT;
            }

        }
        throw new CommonException(CommonErrorCode.NOT_SUPPORT_SOURCE_TYPE,
                String.format("Source type:%s not support!", datasourceType));

    }
}