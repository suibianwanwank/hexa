package observer;

import java.util.List;

public class JobObserverUtil {
    private JobObserverUtil() {
    }

    public static void observerOnJobComplete(List<SqlJobObserver> observers) {
        for (SqlJobObserver observer : observers) {
            observer.onCompleted();
        }
    }

    public static void observerOnDataArrived(List<SqlJobObserver> observers, String data) {
        for (SqlJobObserver observer : observers) {
            observer.onDataArrived(data);
        }
    }

    public static void observerOnException(List<SqlJobObserver> observers, Throwable throwable) {
        for (SqlJobObserver observer : observers) {
            observer.onError(throwable);
        }
    }
}
