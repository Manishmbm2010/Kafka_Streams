package kafka_streams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

public class StreamWithPropertyFile {
	public static void main(String[] args) throws IOException {

		Properties prop = new Properties();
		InputStream in = null;
		in = new FileInputStream("/home/manish/propertyFile");
		prop.load(in);
		
		//prop.put(prop., value)
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		// In the subsequent lines we define the processing topology of the
		// Streams
		// application.
		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> textLines = builder.stream("streams-plaintext-input");
		//builder.a
		//textLines.print();
		final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

		KGroupedStream<String, String> groupByWords = textLines
				.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
				.groupBy((key, word) -> word, Serialized.with(stringSerde, stringSerde));
		groupByWords.count("total-word-count");
		groupByWords.count(SessionWindows.with(20 * 1000 * 1), "Session_window-count");
		groupByWords.count(TimeWindows.of(60 * 1 * 1000), "Windows-word-count");
		final KafkaStreams streams = new KafkaStreams(builder.build(), prop);
		streams.cleanUp();
		streams.start();
		
		//InteractiveQuery iq = new InteractiveQuery(streams,"Interactive Query");
		//iq.start();

		//InteractiveQueryWindowed iqw = new InteractiveQueryWindowed(streams,"Interactive Query");
		///iqw.start();
		
		InteractiveQuerySession iqs = new InteractiveQuerySession(streams,"Interactive Query");
		iqs.start();
	}

}
