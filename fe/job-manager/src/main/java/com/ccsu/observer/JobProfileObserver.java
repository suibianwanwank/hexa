package com.ccsu.observer;

import com.ccsu.profile.JobProfile;
import com.ccsu.profile.ProfileStoreConfig;
import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.StoreManager;
import observer.SqlJobObserver;

public class JobProfileObserver implements SqlJobObserver {

    private DataIndexStore<String, JobProfile> profileStore;

    private JobProfile jobProfile;

    public static JobProfileObserver newJobProfileObserver(JobProfile jobProfile, StoreManager storeManager){
        DataIndexStore<String, JobProfile> dataStore =
                storeManager.getOrCreateDataIndexStore(ProfileStoreConfig.JOB_PROFILE_CONFIG);
        return new JobProfileObserver(dataStore, jobProfile);
    }

    public JobProfileObserver(DataIndexStore<String, JobProfile> profileStore, JobProfile jobProfile) {
        this.profileStore = profileStore;
        this.jobProfile = jobProfile;
    }

    @Override
    public void onCompleted(JobContext context) {
        jobProfile.finish();
        profileStore.put(context.getJobId(), jobProfile);
    }

    @Override
    public void onError(Throwable cause) {

    }

    @Override
    public void onCancel(String cancelCause) {

    }

    @Override
    public void onDataArrived(String data) {

    }
}
