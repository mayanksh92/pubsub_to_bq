# Use the official Python image from the Docker Hub
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Set the environment variable for Google Application Credentials
# ENV GOOGLE_APPLICATION_CREDENTIALS="/app/myservice.json"


# Install the necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the src directory and all its contents into the container
COPY src/ ./src/

# Make the Python script executable
RUN chmod +x ./src/pubsub_to_bq.py

# Run the Python script
ENTRYPOINT ["python", "./src/pubsub_to_bq.py"]

# CMD will be the default arguments, which can be overridden
CMD ["--help"]