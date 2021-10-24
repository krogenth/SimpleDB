package simpledb;

public class LockedException extends RuntimeException {
	LockedException(String message) {
		super(message);
	}
}
