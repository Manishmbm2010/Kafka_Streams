package kafka_streams;

import java.text.SimpleDateFormat;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class InteractiveQueryWindowed implements Runnable {
	Thread t = null;
	KafkaStreams streams = null;
	String queryName;
	Long cnt;
	ReadOnlyWindowStore<String, Long> windowStore = null;
	SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public InteractiveQueryWindowed(KafkaStreams streams, String queryName) {
		this.streams = streams;
		this.queryName = queryName;

	}

	public void run() {
		while (windowStore == null) {
			try {
				// keyValueStore = streams.store("total-word-count",
				// QueryableStoreTypes.<String, Long>keyValueStore());
				windowStore = streams.store("Windows-word-count", QueryableStoreTypes.windowStore());
			} catch (Exception e) {
				System.out.println("Exception in accesing state store");
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					System.out.println("Interupted Exception");
					e1.printStackTrace();
				}
			}
		}
		while (true) {
			try {
				
				long timeFrom = 0; // beginning of time = oldest available
				long timeTo = System.currentTimeMillis(); // now (in
															// processing-time)
				WindowStoreIterator<Long> iterator = windowStore.fetch("word", timeFrom, timeTo);
				
				while (iterator.hasNext()) {
					KeyValue<Long, Long> next = iterator.next();
					String windowTimestamp = sdfDate.format(next.key);
					System.out.println("Windowed Thread Count of 'word' @ time " + windowTimestamp + " is " + next.value);
				}
				System.out.println("****************Next Iteration after 20 seconds*************");
				Thread.sleep(20000);// 20 seconds sleep
				/*System.out.println("Count for Word " + cnt);
				if (cnt > 10) {
					System.out.println("Triggering send mail");
				}*/
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void start() {
		if (t == null) {
			t = new Thread(this, "First Thread");
			if (streams != null) {

				System.out.println("STarting Inteactive Query");
				t.run();
			}
		}
	}
}
