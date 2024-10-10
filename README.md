![MasterHead](https://github.com/user-attachments/assets/b365e9f8-06ec-4035-8640-bb6951adbaa3)


# Netflix Data ETL Pipeline using PySpark and Airflow

<div align = "justify">

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

</div>

# Visualizations

The pipeline generates the following visualizations based on the transformed data:

- **Rating Distribution:** Displays the number of movies per rating.

 <div align = "center">
    <img src="https://github.com/user-attachments/assets/d96652bb-680c-4b7e-b84b-4112533b65af" alt="1" width="50%">
</div> 

- **Category Distribution:** Compares the count of movies vs TV shows.

 <div align = "center">
    <img src="https://github.com/user-attachments/assets/d31f62a9-46df-4af8-bdc8-d711384e7507" alt="1" width="50%">
</div>

- **Top 5 Genres:** Highlights the top 5 most common genres in Netflix content.

 <div align = "center">
    <img src="https://github.com/user-attachments/assets/0a607ba6-8229-4d00-915d-8c694d977d4d" alt="1" width="50%">
</div>

# License

This project is licensed under the MIT License.
