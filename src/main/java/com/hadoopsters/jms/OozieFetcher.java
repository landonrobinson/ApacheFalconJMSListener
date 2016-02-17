package com.hadoopsters.jms;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.util.Date;

/**
 * Connects to Hadoop cluster and captures workflow details stored by Apache Oozie.
 * @author : Landon Robinson
 * @version: 1.0
 * @since: 2016-02-06
 * @last-updated: 2016-02-06
 */

public class OozieFetcher {


    /**
     * @param falconID
     * @param oozieClient
     * @return
     * @throws OozieClientException
     *
     * This method receives a falconID provided by a JMS message, and an Oozie Client instance. It provides
     * a subworkflow ID (the true ID of the workflow you give to falcon).
     */
    static String getSubworkflowID(String falconID, OozieClient oozieClient) throws OozieClientException {

        String id = "";

        //Request Sub-Workflow Action Details from Oozie
        for (WorkflowAction action : oozieClient.getJobInfo(falconID).getActions()){
            if (action.getId().contains("user-action")){
                id = action.getExternalId();
            }
        }
        return id;

    }

    static Date getStartTime(String falconID, OozieClient oozieClient) throws OozieClientException{
        String id = "";
        Date startTime;
        startTime = oozieClient.getJobInfo(falconID).getStartTime();

        return startTime;

    }

    static Date getEndTime(String falconID, OozieClient oozieClient) throws OozieClientException{
        String id = "";

        Date endTime;
        endTime = oozieClient.getJobInfo(falconID).getEndTime();

        return endTime;

    }

    /**
     * @param subworkflowID
     * @param oozieClient
     * @return
     * @throws OozieClientException
     *
     * This method receives a subworkflowID (any ID that has actions is sufficient) and an Oozie Client instance.
     * It provides the duration of said subworkflow. Original use case is for attaching to a JMS message record.
     */
    static String getSubworkflowDuration(String subworkflowID, OozieClient oozieClient) throws OozieClientException {

        String duration = "";

        //Establish PeriodFormatter for Timestamp Output
        PeriodFormatter daysHoursMinutes = new PeriodFormatterBuilder()
                .printZeroAlways()
                .minimumPrintedDigits(2)
                .appendHours()
                .appendLiteral(":")
                .appendMinutes()
                .appendLiteral(":")
                .appendSeconds()
                .toFormatter();

        //Request Sub-Workflow Action Details from Oozie
        WorkflowAction action = oozieClient.getWorkflowActionInfo(subworkflowID);
        Date start = action.getStartTime();
        Date end = action.getEndTime();
        DateTime start_actual = new DateTime(start);
        DateTime end_actual = new DateTime(end);
        Period diff = new Period(start_actual, end_actual);
        duration = daysHoursMinutes.print(diff.normalizedStandard());

        return duration;

    }

}
