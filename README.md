# Project-STEDI-Human-Balance-Analytics

Customer, accelerometer, and step trainer data are all made available on the Data Lakehouse, and can be queried via AWS Athena.

Initial (raw or bronze) data is landed into the _landing tables. These are sourced directly from S3, and have no transformations applied.

Data for research consumption is filtered (through an AWS Glue job) into customer and accelerometer _trusted. These tables contain only data from customers who have consented for their data to be used for research purposes. This layer could be considered silver data.

A final filtering step is applied to the tables. Customer_curated contains the unique details of each customer who has consented for their data to be used for research, and who have accelerometer data available. Step_training_curated contains the step training and matching accelerometer data (joined by timestamp). This table has been sanitized, and contains no personally identifying information. This data is suitable for use in a machine learning project. These tow tables can be considered gold data.
