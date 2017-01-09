# twitterdata-analyis
Perform Twitter Sentimental analysis using mapredce in CDH.

This repository contains an application for analyzing Twitter data using CDH components, including Flume.

Prerequisite to run the application -  Have CDH4 installed. Specifically, you'll need Hadoop and Flume.

1. Configuring Flume

Step 1:
Add the JAR flume-sources-1.0-SNAPSHOT.jar to the Flume classpath. Use the [link](https://drive.google.com/file/d/0B-Cl0IfLnRozUHcyNDBJWnNxdHc/view?usp=sharing) to download.

Step 2:
Move the flume-sources-1.0-SNAPSHOT.jar file from Downloads directory to lib directory of apache flume.
Command: sudo mv Downloads/flume-sources-1.0-SNAPSHOT.jar /usr/lib/flume-ng/lib

Step 3:
Copy flume-env.sh.template content to flume-env.sh
Command: sudo gedit conf/flume-env.sh
Set  FLUME_CLASSPATH as follows:
 FLUME_CLASSPATH = "/usr/lib/flume-ng/lib/flume-sources-1.0-SNAPSHOT.jar"
 
Step 4:

Get twitter credentials from dev.twitter.com/apps and give the keys in the flume.conf file. Sample file is given in the flume folder.
Edit the configuration file using the command sudo gedit /usr/lib/flume-ng/conf/fulme.conf

Step 5:
Start the flume agent using the following command
sudo /usr/bin/flume-ng agent -c /etc/flume-ng/conf -f /etc/flume-ng/conf/flume.conf -n TwitterAgent
Check the given folder in Hue File Browser

2. MapReduce

Use code in the folder mapreducesrc. You can use eclipse provided in Cloudera Quickstart VM. 

1. The output files from flume, which is in JSON format will be given to map method as JSON object in string(Text) format. This is done by setting the input class to MultiLineJsonInputFormat.class
  Reference - https://github.com/alexholmes/json-mapreduce

2. The sentimental  analysis is done using AFINN Dictionary. The words and corresponding score of AFINN is stored in HashMap. Each word in the tweet (except for stop words) will be checked for the words in HashMap. The scores will be added and the final sum determines if the sentiment is positive or negative.



