package com.example.demoservicebus;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



import com.azure.messaging.servicebus.*;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
public class DemoservicebusApplication {

	static String connectionString = "Endpoint=sb://pricesmart-pos-dev2.servicebus.windows.net/;SharedAccessKeyName=vlinero;SharedAccessKey=7CndgfwVMzKUo5Rr6wCHw3pwicc1HVsq4+ASbMnb87c=";
    static String topicName = "tgcp.transactions.complete";
    static String subName = "tlogtest";




	public static void main(String[] args)  {
		SpringApplication.run(DemoservicebusApplication.class, args);

        try {
            receiveMessages();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
	}


    static void receiveMessages() throws InterruptedException
    {
        int numberOfMessagesToPeek = 15; // Adjust the number of messages you want to peek

        CountDownLatch countdownLatch = new CountDownLatch(numberOfMessagesToPeek);

        // Create a receiver using connection string.
        ServiceBusReceiverAsyncClient receiver = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subName)
                .buildAsyncClient();

        receiver.peekMessages(numberOfMessagesToPeek).subscribe(
                message -> {
                    //System.out.println("Received Message Id: " + message.getMessageId());
                    System.out.println("Received Message: " + message.getBody().toString());

                    // Process the message as needed

                    countdownLatch.countDown(); // Decrease the latch count
                },
                error -> System.err.println("Error occurred while receiving message: " + error),
                () -> {
                    System.out.println("Receiving complete.");
                });

        countdownLatch.await(10, TimeUnit.SECONDS); // Wait for the specified time or until all messages are processed

        // Close the receiver
        receiver.close();


    }
    

}
