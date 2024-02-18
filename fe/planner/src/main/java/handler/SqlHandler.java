package handler;


import context.QueryContext;

/**
 * The basic class for handle sql, such as sqlQuery, createRP, refreshRP etc
 */
public interface SqlHandler<RESULT, NODE, CONTEXT extends QueryContext> {
    RESULT handle(NODE sqlNode, CONTEXT context);
}
