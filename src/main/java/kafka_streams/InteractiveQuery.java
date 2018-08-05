package kafka_streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class InteractiveQuery implements Runnable {
	Thread t = null;
	KafkaStreams streams = null;
	String queryName;
	Long cnt;
	ReadOnlyKeyValueStore<String, Long> keyValueStore = null;

	public InteractiveQuery(KafkaStreams streams, String queryName) {
		this.streams = streams;
		this.queryName = queryName;

	}

	public void run() {
		while (keyValueStore == null) {
			try {
				keyValueStore = streams.store("total-word-count", QueryableStoreTypes.<String, Long>keyValueStore());
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
		while(true) {
		try {
			
			cnt =keyValueStore.get("word");
			System.out.println("Count for Word " +cnt );
			if(cnt >10){
				System.out.println("Triggering send mail");
			}
			System.out.println("****************Next Iteration after 20 seconds*************");
			Thread.sleep(20000);// 20 seconds sleep
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
