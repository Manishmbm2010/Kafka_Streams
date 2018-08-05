package kafka_streams;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.admin.NewPartitions;

public class PropertyFileTest {
public static void main(String[] args) throws IOException {
	Properties prop = new Properties();
	InputStream in =  null;
	in = new FileInputStream("/home/manish/propertyFile");
	prop.load(in);
	System.out.println(prop.getProperty("database"));
	System.out.println(prop.getProperty("port"));
}
}
