CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER datafabrik WITH PASSWORD 'datafabrik';
CREATE DATABASE datafabrik OWNER datafabrik;
GRANT ALL PRIVILEGES ON DATABASE datafabrik TO datafabrik;
