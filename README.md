# newyorker_yelp_assignment using SMACK Stack



#NewYorker Yelp Dataset Assignment:

This repository contains the python script code which basically reads the JSON schema files packed inside a TAR file (yelp_dataset_challenge_round9.tar) taken from Yelp Dataset Challenge Round 9.




#Getting Started:

Schema Creation - A method defined in a python script gets called for creating the keyspace and tables.

Parsing of JSON Files Method- The JSON Schema files are one liner objects which are read into a spark dataframe using Spark SQLContext class with method call of json().

Inserting Dataframe into Cassandra Table - The Spark Dataframe holding the JSON data and leveraging the Cassandra package format writes into Cassandra NoSQL Table.

Queries Execution Method - The set of queries used to validate the data inside Cassandra tables are executed from inside function called: execute_queries. Sample queries are written for testing the data semantics and more queries can be written in the same method. 
Idea(Next Steps):I could have created an external sql file encapsulating all queries and including even more and then from the python command line args to only pass argument like: "execute_queries" that could call the method to read the query file. 


#Prerequisites:

Cassandra Spark Connector Package is required to connect with Cassandra Database (com.datastax.spark:spark-cassandra-connector_2.11:2.0.1)
Cassandra Cluster Package should also be installed via: pip install cassandra-driver





#Sample Script Call:

Sample Script Call for newyorker.py from Spark-Submit with 1 arg(filename)

./spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 newyorker.py yelp_dataset_challenge_round9.tar




#Lacking Component:

Packaging of program as a Docker Container - Already declared to Robert Onst in the first interview that the candidate has not worked yet with Dockerizing the application. However, the candidate understands the underlying concept and would like to work in a microservices environment in future.




#Authors:

Muhammad Umair Farooqi




#Acknowledgments:

Hat tip to anyone who's code was used
