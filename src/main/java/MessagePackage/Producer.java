package MessagePackage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.io.*;


public class Producer {

    public static void main(String[] args) throws InterruptedException, IOException {

        String topic = "Kafka-Assignment_y";
        int Id, Age;
        String Name, Course;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        Thread.sleep(1000);
        while (true){
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Press ! to exit or anything else to continue");
            char choice = bufferedReader.readLine().charAt(0);
            if (choice == '!')
                break;
            System.out.println("Enter ID");
            Id = Integer.parseInt(bufferedReader.readLine());
            System.out.println("Enter Name");
            Name = (bufferedReader.readLine());
            System.out.println("Enter Age");
            Age = Integer.parseInt(bufferedReader.readLine());
            System.out.println("Enter Course");
            Course = (bufferedReader.readLine());

            UserMessage userMessage = new UserMessage(Id, Name, Age, Course);
            ProducerRecord producerRecord = new ProducerRecord(topic, userMessage.toString());
            kafkaProducer.send(producerRecord);
            System.out.println("Message\t{ " + userMessage.toString() + " }\t Sent To Consumer");
            Thread.sleep(500);
        }
        kafkaProducer.close();

    }


}
