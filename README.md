# Netflix Data ETL Pipeline using PySpark and Airflow

This project demonstrates a scalable and automated ETL (Extract, Transform, Load) pipeline using PySpark for data transformation and Apache Airflow for workflow orchestration. The pipeline processes Netflix titles data, applying transformations and generating insights, such as distribution of content by rating and genre.

# Features

- **PySpark** for handling large datasets efficiently and performing transformations.
  
- **Airflow DAG** to automate and schedule the ETL pipeline execution.
  
- Data visualizations showcasing insights such as rating distribution, content type distribution and genre trends.

- Clean, modular code structure for easy understanding and modification.

# Setup

- Docker and Docker Compose (for containerized execution with Airflow)

- Visual Studio Code
  
- Python 3.x

- Apache Airflow

- PySpark

# Data Pipeline Workflow

**1. Extract:**

- Reads the Netflix titles dataset (netflix_titles.csv) using PySpark.
  
**2. Transform:**

- Cleans and filters the data, focusing on Netflix content released after 2000.

- Separates the data into movies and TV shows.

- Aggregates data by rating and genre for further analysis.

**3. Load:**

- Saves the transformed data into parquet format.

- Generates and saves visualizations, such as the rating distribution and genre trends.
