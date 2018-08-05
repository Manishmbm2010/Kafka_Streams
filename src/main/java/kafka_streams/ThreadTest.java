package kafka_streams;

class RunnableDemo implements Runnable {

	Thread t = null;
	String threadName = null;

	public RunnableDemo() {
	}

	public RunnableDemo(String threadName) {
		this.threadName = threadName;
		System.out.println("Creating " +  threadName );
	}

	public void run() {
		System.out.println("Running " + this.threadName);
		try {
			for (int i = 4; i > 0; i--) {
				System.out.println("Thread: " + threadName + ", " + i);
				// Let the thread sleep for a while.
				Thread.sleep(50);
			}
		} catch (InterruptedException e) {
			System.out.println("Thread " + threadName + " interrupted.");
		}
		System.out.println("Thread " + threadName + " exiting.");

	}

	public void start() {
		if (t == null) {
			t = new Thread(this, "First Thread");
			t.run();
		}
	}
}

public class ThreadTest {
	public static void main(String[] args) {

		RunnableDemo rd1 = new RunnableDemo("Thread-1");
		rd1.start();

	}
}
