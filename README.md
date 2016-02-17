![alt tag](http://hortonworks.com/wp-content/uploads/2013/09/falcon-logo.png) 
Pigeon - An Apache Falcon JMS Listener
=================
Pigeon is an open source, lightweight Java client that asynchronously listens for and fetches JMS messages sent by Apache Falcon via an MQ service. Built for use with Apache Hadoop, Apache Falcon and Apache Oozie. Ideal if you're using Falcon to schedule jobs on Hadoop, and want a monitoring solution or some documented history of your jobs.

Version 1.0.2 - Oozie Integration
=================
Falcon JMS messages provide good information, but not everything you'd want. There are a few key pieces lacking. That's been resolved in this latest push:
- added Oozie API integration
  - automatically requests the sub-workflow ID of your Falcon job and logs it with the msg
  - automatically requests the start/end times of your sub-workflow, calculates the duration, and logs it with the msg
  - these features allow your messages to provide the info naturally desired with a record of your job's completion
  - to start using it, you simply need to provide the URI/path to your Oozie service (usually looks like        mycluster004.company.com:11000/oozie

How it Works
=================
Falcon has workflows. Those workflows send JMS message alerts upon completion to indicate their success or failure, and other cross-referential information that's very good for monitoring and debugging. Pigeon, when used as a background service, simply retrieves those alerts and writes them to a file. I use this on my Hadoop team to power a monitoring dashboard.

Because this retrieves alerts for every job that runs on the cluster, Pigeon can easily build a job history and lineage on your Hadoop cluster. Give it a shot if you like!

How to Use It
=================
Easy. Pull this code (it's a Maven project). Change the hostname variable to the server where Falcon is installed, and change the filename variable to give Pigeon a text file to write to. As of version 1.2, you can now provide an Oozie address to take advantage of Oozie features (see above). Package the jar and place it on your cluster (can be an edge node). Run it with a hadoop jar command and presto! It will write results to a file in that directory.

nohup hadoop jar FalconJMSListener-1.0-SNAPSHOT-job.jar com.hadoopsters.jms.FalconAsynchronousListener &

Contact Me
=================
Have questions, ideas, or feedback? Email me at landonrobinson92 [@] gmail.com.

Permissions
=================
I appreciate, but do not require, formal attribution. An attribution usually includes the title, author, publisher, and ISBN. For example: “Hadoopsters: Tutorials, Tips and Tricks for Hadoop (https://github.com/landonrobinson/ApacheFalconJMSListener).”

If you feel your use of code examples falls outside fair use or the permission given here, feel free to contact us at hadoopsters@gmail.com.
