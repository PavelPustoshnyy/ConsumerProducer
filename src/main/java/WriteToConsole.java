import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class WriteToConsole {
    public static void main(String[] args) throws IOException {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in));
        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my-fifth-application";
        String topic="first_topic";
        String message = reader.readLine();
        ProducerDemo pd = new ProducerDemo(bootstrapServers,topic,message);
        ConsumerDemo cd = new ConsumerDemo(bootstrapServers,groupId,topic);
    }
}
