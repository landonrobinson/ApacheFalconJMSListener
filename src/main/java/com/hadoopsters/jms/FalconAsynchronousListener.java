package com.hadoopsters.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import javax.jms.*;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Connects to Hadoop cluster and captures JMS alerts generated by Apache Falcon.
 * This version is asynchronous, leveraging an onMessage listener by JMS.
 * @author : Landon Robinson
 * @version: 1.2
 * @since: 2014-12-07
 * @last-updated: 20167-02-06
 */

public class FalconAsynchronousListener {

    // Designate Topic and Port Values (you should only have to change these options)
    // Hostname must be the box where Falcon is generated, in this case it's myhadoopcluster004.mycompany.com
    // Port 61616 is default, unless configured differently
    static String topic = "FALCON.ENTITY.TOPIC";
    static String host = "tcp://myhadoopcluster004:61616";
    static String outputFile = "falcon-jms.log";
    static String oozieHost = "http://lxhdpmastdev002:11000/oozie";


    // Create Connection Variables
    static ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(host);
    static Connection connection;

    public static void main(String[] args) throws Exception {

        // Create Active Connection to Host and Start It
        connection = connectionFactory.createConnection();
        connection.start();
        System.out.println("Connection Opened to " + connectionFactory.getBrokerURL() + " on: " + new java.util.Date() + ", listening for topic " + topic + ".");
        System.out.println("Messages will be written to local file: " + outputFile);

        // Create an Oozie Session
        final OozieClient oozieClient = new OozieClient(oozieHost);

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

                    String entityName = message.getString("entityName");
                    String status = message.getString("status");
                    String operation = message.getString("operation");
                    String feedPaths = message.getString("feedInstancePaths");
                    String parentWorkflow = message.getString("workflowId");
                    String runID = message.getString("runId");
                    String log = message.getString("logFile");
                    String topicName = message.getString("topicName");
                    String brokerTTL = message.getString("brokerTTL");

                    String subworkflow = "";
                    String duration = "";
                    while (subworkflow.isEmpty()){
                        subworkflow = OozieFetcher.getSubworkflowID(parentWorkflow, oozieClient);
                    }
                    while (duration.isEmpty()){
                        duration = OozieFetcher.getSubworkflowDuration(subworkflow, oozieClient);
                    }

                    // Log the Message Contents (its important parts)
                    // SUPPORTED VALUES:
                    // entityName, feedNames, feedInstancePath, workflowId, runId,
                    // nominalTime, timeStamp, brokerUrl, brokerImplClass, entityType,
                    // operation, logFile, topicName, status, brokerTTL
                    PrintWriter out = new PrintWriter(new FileWriter(outputFile, true));
                    try{
                        out.println("Time: " + currTimeStamp
                                + ", Entity Name: " + entityName
                                + ", Status: " + status
                                + ", Operation: " + operation
                                + ", Feed Instance: " + feedPaths
                                + ", JobID: " + parentWorkflow
                                + ", RunID: " + runID
                                + ", Sub-Workflow: " + subworkflow
                                + ", Duration: " + duration
                                + ", Log: " + log
                                + ", Topic: " + topicName
                                + ", Broker: " + brokerTTL
                        );
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
                } catch (OozieClientException e) {
                    e.printStackTrace();
                }
            }
        });

    }

}
