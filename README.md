# ETL Toll Data Pipeline with Airflow

This project implements an Extract, Transform, Load (ETL) pipeline for toll data using Apache Airflow. The pipeline is designed to download a zip file containing multiple datasets with different extensions, extract data from these sources, perform various data processing tasks, and load the transformed data into PostgreSQL. Additionally, there's an example commented-out section for integrating Kafka into the pipeline for data streaming.

## Project Components
The project utilizes the following components:

- **Airflow DAG (Directed Acyclic Graph)**: The main orchestrator that defines the workflow of tasks.
- **BashOperator**: Used for tasks that involve running shell commands, such as downloading the dataset.
- **PythonOperator**: Employed for executing Python functions within the workflow, allowing for more complex data processing.
- **Pandas**: Utilized for data manipulation and processing.
- **SQLAlchemy**: Employed to connect to and interact with a PostgreSQL database.
- **Confluent Kafka**: Demonstrated as a potential integration for data streaming. Note: You need a running Kafka broker and a defined topic for this section.

## The Outcme
The pipeline is configured to run on a daily basis, with the resulting data being loaded into PostgreSQL. This data is subsequently accessible for analysis and visualization.

The ELT pipeline is depicted in the following graph:
![Airflow_graph](/Airflow_graph.jpg "Screenshot of the Airflow_graph")

The destination where the data is loaded is depicted below:
![Database](/Screenshot_DB.jpg "Screenshot of the Database")


