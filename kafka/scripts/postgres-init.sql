-- Create the airflow user
CREATE USER airflow WITH PASSWORD 'airflow';

-- Create the airflow database
CREATE DATABASE airflow WITH OWNER = airflow;

-- Grant privileges to the airflow user
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;