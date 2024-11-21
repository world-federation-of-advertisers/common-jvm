##!/bin/bash
#
## Pull the latest Pub/Sub emulator Docker image
#docker pull gcr.io/google.com/cloudsdktool/cloud-sdk:emulators
#
## Run the Pub/Sub emulator in a Docker container
#docker run -d \
#  --name pubsub-emulator \
#  -p 8085:8085 \
#  gcr.io/google.com/cloudsdktool/cloud-sdk:emulators \
#  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
#
## Wait for the emulator to start
#sleep 5
#
## Set the PUBSUB_EMULATOR_HOST environment variable
#export PUBSUB_EMULATOR_HOST=localhost:8085
#
## Execute the command passed to this script
#"$@"
#
## Stop and remove the emulator container after the command completes
#docker stop pubsub-emulator
#docker rm pubsub-emulator

#!/bin/bash
echo "start_emulator.sh: Starting Pub/Sub emulator..."

if ! docker stop pubsub-emulator 2>/dev/null; then
  echo "start_emulator.sh: No running pubsub-emulator container to stop."
fi

if ! docker rm pubsub-emulator 2>/dev/null; then
  echo "start_emulator.sh: No existing pubsub-emulator container to remove."
fi

# Stop any existing containers
#docker stop pubsub-emulator 2>/dev/null
#docker rm pubsub-emulator 2>/dev/null
#
## Run the Pub/Sub emulator
#docker run -d \
#  --name pubsub-emulator \
#  -p 8085:8085 \
#  gcr.io/google.com/cloudsdktool/cloud-sdk:emulators \
#  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
#
## Wait for the emulator to be ready
#max_attempts=30
#attempt=1
#while ! nc -z localhost 8085; do
#  if [ $attempt -gt $max_attempts ]; then
#    echo "Emulator failed to start after $max_attempts attempts"
#    exit 1
#  fi
#  echo "Waiting for emulator to start (attempt $attempt)..."
#  sleep 1
#  ((attempt++))
#done
#
#echo "Emulator is ready!"
#export PUBSUB_EMULATOR_HOST=localhost:8085
#
## Execute the command
#exec "$@"