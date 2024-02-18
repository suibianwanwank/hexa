package com.ccsu.resource.planner;

import com.ccsu.Result;
import com.ccsu.manager.SqlJobManager;
import com.ccsu.resource.planner.pojo.SqlApiSubmitRequest;
import com.ccsu.session.UserRequest;
import com.google.inject.Inject;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import java.util.concurrent.ExecutionException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/planner")
public class SqlPlannerResource {

    SqlJobManager sqlJobManager;

    @Inject
    SqlPlannerResource(SqlJobManager sqlJobManager) {
        this.sqlJobManager = sqlJobManager;
    }

    @POST
    @Path("/parse/planner")
    @Consumes(APPLICATION_JSON)
    public String submitSqlAndReturnPlanner(
            SqlApiSubmitRequest request) throws ExecutionException, InterruptedException {

        UserRequest userRequest = new UserRequest();
        userRequest.setClusterId(request.getClusterId());

        Result result = sqlJobManager.submitSqlJob(userRequest, request.getSql()).get();

        return result.getResult().toString();

    }
}
