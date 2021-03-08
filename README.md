# Microservices Saga Demo

This demo is useful for showing microservices communicating through a Redis Streams messaging bus.  It can randomly inject failures that will be cleaned up and retried.

## Running Locally

Set up your microservices flow - See below for configuration infomation

```
# Copy the example microservices flow
cp conf/config.yml.example conf/config.yml

```

Use docker-compose

```
docker-compose up
```

[Use the Web UI](http://localhost:5000)

[Redis Insight](http://localhost:8001)

## Configuration Information

```
# Set your redis host - use redis for docker
host: redis
# Set your redis port - use 6379 for docker
port: 6379

# create a pipeline of microservices

microservices:
#  The initial service - listens on the stream with the name
 - name: kickoff
   output: step1

# The next service in the pipeline
 - name: OrderAccepted
   input: step1
   output: step2
   batch_size: 2 
   # How many messsages to pull from the queue 
   # at a time - default is 1

 - name: OrderQueued
   input: step2
   output: step3
   min_proc_ms: 0
   max_proc_ms: 2
   # Sleep a random amount of ms
   # between the min and max to simulate processing time

 - name: OrderExecuted
   input: step3
   output: step4
   error_rate_percent: 0.2
   # Have this pipeline step fail 
   # a certain percentage of the time 

#  The final service
 - name: OrderConfirmed
   input: step4  
```