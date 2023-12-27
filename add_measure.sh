#!/bin/bash

# Configuration
redis_host="localhost"
redis_port="6379"

redis_key="my_key"
redis_value="my_value"

start_time=$(date +%s%N)

# Insert key-value
redis-cli -h $redis_host -p $redis_port SET $redis_key "$redis_value"

end_time=$(date +%s%N)

# Time in nanoseconds
echo "$(($end_time - $start_time))"
