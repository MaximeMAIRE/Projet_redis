#!/bin/bash

# Connect to Redis and retrieve data
value1=$(redis-cli get 'key1')
value2=$(redis-cli get 'key2')

# Print the values
echo "Value for key1: $value1"
echo "Value for key2: $value2"

