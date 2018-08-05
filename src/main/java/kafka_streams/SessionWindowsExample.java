package kafka_streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class SessionWindowsExample {

	  static final String PLAY_EVENTS = "streams-plaintext-input";
	  static final Long INACTIVITY_GAP = TimeUnit.MINUTES.toMillis(2);
	  static final String PLAY_EVENTS_PER_SESSION = "streams-wordcount-output";

	  public static void main(String[] args) {
	    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
	    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
	    final KafkaStreams streams = createStreams(bootstrapServers,
	                                               schemaRegistryUrl,
	                                               "/tmp/kafka-streams");

	    // Always (and unconditionally) clean local state prior to starting the processing topology.
	    // We opt for this unconditional call here because this will make it easier for you to play around with the example
	    // when resetting the application for doing a re-run (via the Application Reset Tool,
	    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
	    //
	    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
	    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
	    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
	    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
	    // See `ApplicationResetExample.java` for a production-like example.
	    streams.cleanUp();

	    streams.start();

	    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	  }

	  static KafkaStreams createStreams(final String bootstrapServers,
	                                    final String schemaRegistryUrl,
	                                    final String stateDir) {
	    final Properties config = new Properties();
	    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
	    // against which the application is run.
	    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-windows-example");
	    config.put(StreamsConfig.CLIENT_ID_CONFIG, "session-windows-example-client");
	    // Where to find Kafka broker(s).
	    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
	    // Set to earliest so we don't miss any data that arrived in the topics before the process
	    // started
	    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    // disable caching to see session merging
	    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

	    // create and configure the SpecificAvroSerdes required in this example
	    /*final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
	    final Map<String, String> serdeConfig = Collections.singletonMap(
	        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
	    playEventSerde.configure(serdeConfig, false);
*/
	    final Serde<String> stringSerde = Serdes.String();
	    final Serde<Long> longSerde = Serdes.Long();
	    final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
	    
	    final StreamsBuilder builder = new StreamsBuilder();
	    KStream<String, String> textLines = builder.stream(PLAY_EVENTS, Consumed.with(Serdes.String(), Serdes.String()));
	    
	    textLines
	    		.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
	  	      .selectKey((key, word) -> word).groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
	  	      .windowedBy(SessionWindows.with(INACTIVITY_GAP))
	  	      .count().toStream()
		        // map key to a readable string
		        .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
		        // write to play-events-per-session topic
		        .to(PLAY_EVENTS_PER_SESSION, Produced.with(Serdes.String(), Serdes.Long()));;
	    
	        // group by key so we can count by session windows
	      /*  .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
	        // window by session
	        .windowedBy(SessionWindows.with(INACTIVITY_GAP))
	        // count play events per session
	        .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(PLAY_EVENTS_PER_SESSION)
	            .withKeySerde(Serdes.String())
	            .withValueSerde(Serdes.Long()))
	        // convert to a stream so we can map the key to a string
	        .toStream()
	        // map key to a readable string
	        .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
	        // write to play-events-per-session topic
	        .to(PLAY_EVENTS_PER_SESSION, Produced.with(Serdes.String(), Serdes.Long()));
*/
	    return new KafkaStreams(builder.build(), new StreamsConfig(config));
	  }

	}
