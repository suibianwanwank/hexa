package handler;

/**
 * Used to represent a no return result type.
 */
public class Void {
    public final static Void DEFAULT = new Void();

    private Void() {
    }

    @Override
    public String toString() {
        return "ok";
    }
}
