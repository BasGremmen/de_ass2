# Using python 3.7 full image as the base image
FROM python:3.7.7-slim
# Working Directory
WORKDIR /usr/src/myapp
# Copy requirements.txt  to the working directory
COPY requirements.txt .

# Install required python packages
RUN pip install --no-cache-dir -r requirements.txt
# Copy all files
COPY . .
# Expose the port that our app runs in
EXPOSE 5000

# Run our App
CMD ["python3","app.py"]
