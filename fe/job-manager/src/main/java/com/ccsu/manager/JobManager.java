package com.ccsu.manager;

import observer.SqlJobObserver;
import com.ccsu.session.UserRequest;

import java.util.List;


public interface JobManager {
    void cancelSqlJob();

    void submitSqlJob(UserRequest userRequest, String sql, List<SqlJobObserver> observers);
}
