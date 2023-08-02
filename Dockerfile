# Use an official Python runtime as a parent image
FROM python:3.11.4

# Install Firefox
RUN apt-get update && \
    apt-get install -y firefox-esr

# Create a folder for our code
RUN mkdir event-fetcher
RUN chmod 777 event-fetcher

# Set the working directory in the container to /event-fetcher
WORKDIR /event-fetcher

# Copy the current directory contents into the container at /event-fetcher
COPY . ./

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Run events.py when the container launches
CMD ["python", "events.py"]