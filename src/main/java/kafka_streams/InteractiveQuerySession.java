package kafka_streams;

import java.text.SimpleDateFormat;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class InteractiveQuerySession implements Runnable {
	Thread t = null;
	KafkaStreams streams = null;
	String queryName;
	Long cnt;
	ReadOnlySessionStore<String, Long> sessionStore = null;
	SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public InteractiveQuerySession(KafkaStreams streams, String queryName) {
		this.streams = streams;
		this.queryName = queryName;

	}

	public void run() {
		while (sessionStore == null) {
			try {
				// keyValueStore = streams.store("total-word-count",
				// QueryableStoreTypes.<String, Long>keyValueStore());
				sessionStore = streams.store("Session_window-count", QueryableStoreTypes.sessionStore());
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
				KeyValueIterator<Windowed<String>, Long> iterator = sessionStore.fetch("word");
				KeyValue<Windowed<String>, Long> next = null;
				while (iterator.hasNext()) {
					next = iterator.next();
					if (iterator.hasNext() == false && next != null) {
						String windowStart = sdfDate.format(next.key.window().start());
						String windowEnd = sdfDate.format(next.key.window().end());
						System.out.println("Count of 'word' @  SessionStartTime : " + windowStart + " SessionEndTime "
								+ windowEnd + "    " + next.value);
						if(next.value > 10){
							System.out.println("Trigger Send Mail");
						}
					}
				}
				System.out.println("****************Next Interation after 20 seconds*************");
				Thread.sleep(20000);// 20 seconds sleep
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void start() {
		if (t == null) {
			t = new Thread(this, "Session Thread");
			if (streams != null) {

				System.out.println("Session Thread : Starting Inteactive Query");
				t.run();
			}
		}
	}
}
