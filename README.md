# Bahmni Sync Development Environment setup

## Prerequisites
 - Oracle JDK 8
 - OpenMRS SDK
 - Docker is installed and running.
 
## Setting up Debezium
 Using Debezium requires three separate services: ZooKeeper, Kafka, and the Debezium connector service. You will set up a single instance of each service using Docker and the Debezium container images.
 
 ### Starting Zookeeper
    docker run -d --name zookeeper -p 2181:2181 jplock/zookeeper
    
 ### Starting Kafka
    docker run -d --name kafka -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=<private-ip> -e ZOOKEEPER_IP=<private-ip> ches/kafka
    
  - replace <private-ip> with your system's private IP
  
 ### Starting a MySQL database
   At this point, you have started ZooKeeper and Kafka, but you still need a database server from which Debezium can capture changes.
 
    docker run -d --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql
    
  - -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw
     - Creates a user and password that has the minimum privileges required by the Debezium MySQL connector.  
     
  - Unzip openmrs_dump_20210618_clean.zip and import openmrs db to the just started mysql server 
 
*binglog is already enabled for the debzium mysql docker used above. Incase you are using different mysql server you would need to enable binlog yourselves. Follow instructions [here](https://documentation.commvault.com/commvault/v11/article?p=34667.htm) to enable it.*
     
 ### Starting Kafka Connect
      docker run -d --name connect -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my_connect_configs -e OFFSET_STORAGE_TOPIC=my_connect_offsets -e STATUS_STORAGE_TOPIC=my_connect_statuses --link zookeeper:zookeeper --link kafka:kafka --link mysql:mysql --link mysql:mysql debezium/connect
      
 ### Registering a connector to monitor the openmrs database 
 Open a new terminal, and use the curl command to register the Debezium MySQL connector.
 
      $ curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "openmrs-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "inventory", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.inventory" } }'

 or make a POST call through postman
 
     http://localhost:8083/connectors/
 
 body 
 
    {
      "name": "openmrs-connector",  
      "config": {  
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",  
        "database.hostname": "mysql",  
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "dbz",
        "database.server.id": "184054",  
        "database.server.name": "dbserver1",  
        "database.include.list": "inventory",  
        "database.history.kafka.bootstrap.servers": "kafka:9092",  
        "database.history.kafka.topic": "schema-changes.inventory"  
      }
    }

 *For more details see Debezium Tutorial [here](https://debezium.io/documentation/reference/tutorial.html)*
 
 
## Setting up OpenMRS SDK Dev Environment
 
 *To Setup the OpenMRS Server for the Developement follow instructions [here](https://wiki.openmrs.org/display/docs/OpenMRS+SDK)*
 
 Clone project on your system. Let's build, deploy and run the module on the server. From the project directory:
 
     mvn clean install openmrs-sdk:run
 
 Repeat for both projects. You will need to create two different server for each project. 
 
 
## Setting up Required Global Properties in OpenMRS

 Both modules have a configuration page that needs to be completed before you can use it for the syncing purposes. 
 
  ### Worker Module Properties
 
  | Property name | Description | Default | 
| --- | --- | --- |
| bahmnisyncworker.worker.node.id | Worker ID, should be be unique among all the nodes synchronizing to master | - |
| bahmnisyncworker.master.url | Master url for data sync | - |
| bahmnisyncworker.kafka.url | Kafka Server url | localhost:9092 |
| bahmnisyncworker.sync.chunk.size | Maximum number of changes per request | 2 |
| bahmnisyncworker.database.server.name | Database Server Name | dbserver1 |
| bahmnisyncworker.openmrs.schema.name | OpenMRS Schema Name | openmrs |
| bahmnisyncworker.master.node.user | Bahmni Sync Master Node username with Bahmni Sync privileges | superman |
| bahmnisyncworker.master.node.password | Bahmni Sync Master Node password | Admin123 |
 
 ### Master Module Properties
 
 | Property name | Description | Default | 
| --- | --- | --- |
| bahmnisyncmaster.kafka.url | Kafka Server url | localhost:9092 |
| bahmnisyncmaster.database.server.name | Database Server Name | dbserver1 |
| bahmnisyncmaster.openmrs.schema.name | OpenMRS Schema Name | openmrs | 
| bahmnisyncmaster.conflict.resolution.rule | options: latest date_updated, earliest date_updated, master always, worker always | latest date_updated | 
 
## User privileges Required
 
 - Manage Bahmni Sync 

