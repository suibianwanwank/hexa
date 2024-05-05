package observer;

import com.ccsu.profile.JobProfile;

public interface SqlJobObserver {

    /**
     * This is called when the sql job execution completes.
     *
     */
    void onCompleted(JobContext context);

    /**
     * This method is called when an exception occurs in the sql job.
     *
     * @param cause The exception that occurred.
     */
    void onError(Throwable cause);

    /**
     * This is called when the sql job cancel.
     *
     * @param cancelCause cancel reason.
     */
    void onCancel(String cancelCause);

    /**
     * This is called when data arrived.
     *
     * @param data arrived data.
     */
    void onDataArrived(String data);


    class JobContext{
        private final String jobId;
        private final JobProfile jobProfile;

        public JobContext(String jobId, JobProfile jobProfile) {
            this.jobId = jobId;
            this.jobProfile = jobProfile;
        }

        public String getJobId() {
            return jobId;
        }

        public JobProfile getJobProfile() {
            return jobProfile;
        }
    }
}
