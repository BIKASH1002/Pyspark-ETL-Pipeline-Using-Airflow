# Use the puckel/docker-airflow image as base
FROM puckel/docker-airflow:1.10.1

# Install matplotlib and any other dependencies you may need
RUN pip install matplotlib


