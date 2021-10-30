package simpledb;

public class DeadlockException extends Exception {
	DeadlockException(Exception e) {
		super(e);
	}
}
