# ETL Toll Data Pipeline with Airflow

This project implements an Extract, Transform, Load (ETL) pipeline for toll data using Apache Airflow. The pipeline is designed to download a dataset, perform various data processing tasks, and load the transformed data into PostgreSQL. Additionally, there's an example commented-out section for integrating Kafka into the pipeline for data streaming.

## Project Components
The project utilizes the following components:

- **Airflow DAG (Directed Acyclic Graph)**: The main orchestrator that defines the workflow of tasks.
- **BashOperator**: Used for tasks that involve running shell commands, such as downloading the dataset.
- **PythonOperator**: Employed for executing Python functions within the workflow, allowing for more complex data processing.
- **Pandas**: Utilized for data manipulation and processing.
- **SQLAlchemy**: Employed to connect to and interact with a PostgreSQL database.
- **Confluent Kafka**: Demonstrated as a potential integration for data streaming. Note: You need a running Kafka broker and a defined topic for this section.

## The Result
The pipeline is designed to run daily, and the data is loaded into PostgreSQL. The data is then available for analysis and visualization.
The ELT pipeline is shown below:
![Airflow_graph](/Airflow_graph.jpg "Screenshot of the Airflow_graph")
The tablin the data sink is shown below:
![Database](/Screenshot_DB "Screenshot of the Database")


