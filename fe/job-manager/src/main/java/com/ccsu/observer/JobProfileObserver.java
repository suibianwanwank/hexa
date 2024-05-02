package com.ccsu.observer;

import com.ccsu.profile.JobProfile;
import com.ccsu.profile.ProfileStoreConfig;
import com.ccsu.store.api.DataIndexStore;
import com.ccsu.store.api.StoreManager;
import observer.SqlJobObserver;

public class JobProfileObserver implements SqlJobObserver {

    private DataIndexStore<String, JobProfile> profileStore;

    private String jobId;

    private JobProfile jobProfile;

    public static JobProfileObserver newJobProfileObserver(String jobId, JobProfile jobProfile, StoreManager storeManager){
        DataIndexStore<String, JobProfile> dataStore =
                storeManager.getOrCreateDataIndexStore(ProfileStoreConfig.JOB_PROFILE_CONFIG);
        return new JobProfileObserver(dataStore, jobId, jobProfile);
    }

    public JobProfileObserver(DataIndexStore<String, JobProfile> profileStore, String jobId, JobProfile jobProfile) {
        this.profileStore = profileStore;
        this.jobId = jobId;
        this.jobProfile = jobProfile;
    }

    @Override
    public void onCompleted() {
        jobProfile.finish();
        profileStore.put(jobId, jobProfile);
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
