package handler;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.option.Option;
import com.ccsu.option.OptionManager;
import com.ccsu.option.Scope;
import com.ccsu.option.definition.OptionDefinition;
import com.ccsu.parser.sqlnode.SqlSetConfig;
import com.sun.org.apache.bcel.internal.generic.GETFIELD;
import context.QueryContext;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

public class SetOptionHandler
        implements SqlHandler<Void, SqlSetConfig, QueryContext> {

    @Override
    public Void handle(SqlSetConfig sqlNode, QueryContext context) {


        OptionManager optionManager = context.getOptionManager();

        Scope scope = Scope.valueOf(sqlNode.getScope().getSimple());

        OptionManager scopeOption = getOptionManagerByScope(scope, optionManager);


        SqlNode key = sqlNode.getEntry().getKey();


        Optional<OptionDefinition> optionDefinition = optionManager.getOptionDefinition(key.toString());

        if (!optionDefinition.isPresent()) {
            throw new CommonException(CommonErrorCode.OPTION_STORE_ERROR, String.format("Not exist option:%s", key));
        }

        SqlNode value = sqlNode.getEntry().getValue();

        optionManager.setOption(optionDefinition.get(), value.toString());

        return Void.DEFAULT;
    }

    private OptionManager getOptionManagerByScope(Scope scope, OptionManager current) {
        if (current.getScope() == scope) {
            return current;
        }
        if (current.getParent() == null) {
            throw new CommonException(CommonErrorCode.OPTION_SCOPE_ERROR, String.format("Not find scope:%s option manager", scope.name()));
        }
        return getOptionManagerByScope(scope, current.getParent());
    }

}
