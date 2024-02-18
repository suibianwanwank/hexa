package observer;

public interface SqlJobObserver {

    /**
     * This is called when the sql job execution completes.
     *
     */
    void onCompleted();

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
}
