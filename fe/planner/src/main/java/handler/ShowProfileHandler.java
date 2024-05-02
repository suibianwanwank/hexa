package handler;

import com.ccsu.error.CommonException;
import com.ccsu.parser.sqlnode.SqlShowProfile;
import com.ccsu.profile.JobProfile;
import com.ccsu.profile.ProfileStoreConfig;
import com.ccsu.store.api.DataStore;
import com.ccsu.store.api.EntityWithTag;
import com.ccsu.store.api.StoreConfig;
import com.ccsu.store.api.StoreManager;
import context.QueryContext;

import static com.ccsu.error.CommonErrorCode.JOB_PROFILE_NOT_EXIST;

public class ShowProfileHandler
        implements SqlHandler<String, SqlShowProfile, QueryContext> {
    @Override
    public String handle(SqlShowProfile sqlNode, QueryContext context) {
        StoreManager storeManager = context.getStoreManager();

        DataStore<String, JobProfile> dataStore
                = storeManager.getOrCreateDataStore(ProfileStoreConfig.JOB_PROFILE_CONFIG);

        String jobId = sqlNode.getJobId().getSimple();

        EntityWithTag<String, JobProfile> profileEntityWithTag = dataStore.get(jobId);

        if (profileEntityWithTag == null
                || profileEntityWithTag.getValue() == null) {
            throw new CommonException(JOB_PROFILE_NOT_EXIST, String.format("Not Exist profile with job id:\"%s\"", jobId));
        }

        JobProfile profile = profileEntityWithTag.getValue();

        return profile.formatProfileTable();
    }
}
