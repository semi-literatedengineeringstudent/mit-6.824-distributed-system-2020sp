#!/bin/bash

# Number of parallel instances to run
num_instances=$1

# Command to execute 
command="go test -race -run 2C" 

# Run the command in parallel
for i in $(seq 1 $num_instances); do
    $command &  # The & runs the command in the background
done

# Wait for all background processes to finish
wait