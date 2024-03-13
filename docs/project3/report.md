# git-repo-URL: https://github.com/lyz-sys/neu-cs6650-projects-v2

# Description of Database Designs and Deployment Topologies on AWS
Projects 3's code is located at src/main/java/project3. The major changes are in project3/server/consumers/Driver.java and project3/server/db/DynamoDBController.java file. In these file, I add dynamoDB update function after the information is stored at concurrent hash map. 

The database has one table. The primary key is skierId, and other attributes are verticals, time, liftID, dayID, seasonID, resortID. This should be sufficient enough to handle later queries in assignment 4. 

The deployment topology is same as assignment 2, which is like:      
client -> aws load balencer -> (2 * EC2 hosting Tomcat server) -> rmq -> consumer -> dynamo db

# Metric Results
client metrics
![](clientInfo.png)

rmq console profile
![](rmqInfo1.png)

![](rmqInfo2.png)

dynamo db profile

the current item count as the program running: 
![](dbInfo.png)

the example db items:
![](dbItems.png)
