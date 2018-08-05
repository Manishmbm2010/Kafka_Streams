package kafka_streams;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

public class sessionWindowCount {
	public static void main(final String[] args) throws Exception {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// Specify default (de)serializers for record keys and for record
		// values.
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		// In the subsequent lines we define the processing topology of the
		// Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> textLines = builder.stream("streams-plaintext-input");
		// builder.a
		// textLines.print();
		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

		KGroupedStream<String, String> groupByWords = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				.groupBy((key, word) -> word, Serialized.with(stringSerde, stringSerde));
		groupByWords.count("total-word-count");
		groupByWords.count(SessionWindows.with(20 * 1000 * 1), "Session_window-count");
		groupByWords.count(TimeWindows.of(60 * 1 * 1000), "Windows-word-count");
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();

		// InteractiveQuery iq = new InteractiveQuery(streams,"Interactive
		// Query");
		// iq.start();

		// InteractiveQueryWindowed iqw = new
		// InteractiveQueryWindowed(streams,"Interactive Query");
		/// iqw.start();

		// InteractiveQuerySession iqs = new
		// InteractiveQuerySession(streams,"Interactive Query");
		// iqs.start();

		AllSessionInteractiveQuery iqs = new AllSessionInteractiveQuery(streams, "Interactive Query");
		iqs.start();
	}
}
