# CDC (CHANGE DATA CAPTURE)
**Change Data Capture / Replication on Going using PySpark and AWS Services such as S3, RDS, DMS, LAMBDA, GLUE etc.**

## Project Description Summary

The proposed project aims to implement a change data capture (CDC) system that captures and replicates ongoing changes in a database to another storage system. The primary objective of this project is to create a data leak out of the database and track every change that occurs, including 
- Insertion, 
- Deletion, 
- Modification

The CDC system will involve developing a pipeline that continuously monitors the database and sends the changes to a file or another HDFC storage system. The pipeline will be responsible for identifying and recording all modifications in the database and forwarding them to the storage system for storage and future retrieval.

The project will require the creation of a detailed architecture for the CDC system from scratch. The architecture will encompass all aspects of the system, including data flow, data transformation, and data storage. The system will be designed to be scalable, flexible, and robust to handle large amounts of data and accommodate any changes that may occur in the future.

Overall, the proposed project will provide an efficient and reliable solution for capturing and replicating ongoing changes in a database to another storage system. It will help organizations to monitor their databases and ensure that all changes are tracked and recorded accurately, thus enhancing data management and decision-making processes.


## Architecture

The project creates a pipeline to capture changes in a database and replicate those changes to another storage system. The architecture will involve several components, including:

- Mythical database
- RDF database
- Amazon RDS Relational Database Service
- S3 bucket
- Data Migration Service (DMS)

1. The first step in the architecture is to load all the existing data from the RDS into the S3 bucket. This is referred to as the "full load." Once this is complete, the pipeline will capture any changes that occur in the RDS using a process called change data capture (CDC). The changes will be written to the RDS database and Temporary HDFS / S3 Storage, which acts as a buffer between the RDS and the S3 bucket.

2. The DMS is responsible for moving data between the RDS database and the S3 bucket. 
It has two endpoints: 
- one for reading data from the RDS database and 
- one for writing data to the S3 bucket. 
The DMS will periodically read data from the RDS database and write it to the S3 bucket. This process will capture any changes that have occurred in the mythical database since the last time the DMS ran.

3. The S3 bucket is the final destination for the replicated data. The architecture includes a temporary bucket to hold data during the replication process. Once the data has been successfully replicated to the S3 bucket, it can be used for analysis or other purposes.


## Installation

To install and run the project, follow these steps:

1. Clone the repository
2. Create an AWS account and configure the required services, including:
  - Amazon RDS
  - S3 bucket
  - Data Migration Service (DMS)
3. Install PySpark and the required Python libraries
4. Run the CDC pipeline using PySpark and AWS services

For more information on how to configure and use the project, please refer to the documentation in

## CDC Architecture
Overall, the architecture appears to be designed to capture changes in a database and replicate those changes to another storage system in a reliable and efficient way. The use of a buffer (the RDF database) and a data migration service (DMF) helps to ensure that the replication process is robust and can handle large amounts of data.
![Change Data Capture Architecture / Replication On going]
()
