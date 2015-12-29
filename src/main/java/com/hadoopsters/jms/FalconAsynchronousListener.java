package com.hadoopsters.jms;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Connects to Hadoop cluster and captures JMS alerts generated by Apache Falcon.
 * This version is asynchronous, leveraging an onMessage listener by JMS.
 * @author : Landon Robinson
 * @version: 1.1
 * @since: 2014-12-07
 * @last-updated: 2015-12-15
 */

public class FalconAsynchronousListener {

    // Designate Topic and Port Values (you should only have to change these options)
    // Hostname must be the box where Falcon is generated, in this case it's myhadoopcluster004.mycompany.com
    // Port 61616 is default, unless configured differently
    static String topic = "FALCON.ENTITY.TOPIC";
    static String host = "tcp://myhadoopcluster004:61616";
    static String outputFile = "falcon-jms.log";

    // Create Connection Variables
    static ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(host);
    static Connection connection;

    public static void main(String[] args) throws Exception {

        // Create Active Connection to Host and Start It
        connection = connectionFactory.createConnection();
        connection.start();
        System.out.println("Connection Opened to " + connectionFactory.getBrokerURL() + " on: " + new java.util.Date() + ", listening for topic " + topic + ".");
        System.out.println("Messages will be written to local file: " + outputFile);

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a destination (Topic or Queue, Though Falcon Uses a Topic)
        Destination destination = session.createTopic(topic);

        // Create a MessageConsumer from the Session to the Topic
        MessageConsumer consumer = session.createConsumer(destination);

        // Create a MessageListener from
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message messageRaw) {
                try {
                    String messageText;
                    if (messageRaw instanceof TextMessage) {
                        messageText = ((TextMessage) messageRaw).getText();
                    } else{
                        messageText = messageRaw.toString();
                    }

                    System.out.println("Message Received: " + messageText);
                    MapMessage message = (MapMessage) messageRaw;

                    // Setup Timestamp for Attaching to Message Output
                    java.util.Date date = new java.util.Date(message.getJMSTimestamp());
                    java.sql.Timestamp currTimeStamp = new java.sql.Timestamp(date.getTime());

                    // Log the Message (just the important parts)
                    PrintWriter out = new PrintWriter(new FileWriter(outputFile, true));
                    try{
                        out.println("Time: " + currTimeStamp
                                + ", Entity Name: " + message.getString("entityName")
                                + ", Status: " + message.getString("status")
                                + ", Operation: " + message.getString("operation")
                                + ", Feed Instance: " + message.getString("feedInstancePaths")
                                + ", JobID: " + message.getString("workflowId")
                                + ", RunID: " + message.getString("runId"));
                        out.close();

                    } catch (NullPointerException n) {
                        System.err.println("Caught exception " + n);
                    }
                    out.close();

                    //Handle JMS Exceptions from MapMessages and IO Exceptions from File Writing
                } catch (JMSException jmse){
                    jmse.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

    }

}
