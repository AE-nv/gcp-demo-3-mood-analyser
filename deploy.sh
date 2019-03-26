#!/bin/bash

mvn exec:java -Dexec.mainClass="be.ae.googleclouddemo.GeneralMoodPipeline" -e -Dexec.args="--project=message-board-c2da6 --sinkProject=message-board-c2da6 --stagingLocation=gs://message-board --runner=DataflowPipelineRunner --streaming=true --numWorkers=1 --zone=europe-west1-b"
