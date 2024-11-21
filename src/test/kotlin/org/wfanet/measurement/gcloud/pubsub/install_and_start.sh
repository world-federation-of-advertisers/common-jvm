#!/bin/bash
set -e

# Set installation directory for gcloud
GCLOUD_DIR="${HOME}/google-cloud-sdk"
GCLOUD_BIN="${GCLOUD_DIR}/bin/gcloud"

# Install gcloud CLI if not already installed
if [ ! -f "${GCLOUD_BIN}" ]; then
    echo "Installing Google Cloud SDK..."
    curl -sSL https://sdk.cloud.google.com | bash > /dev/null
    export PATH="${GCLOUD_DIR}/bin:$PATH"
else
    echo "Google Cloud SDK is already installed."
fi

# Install Pub/Sub emulator component
echo "Installing Pub/Sub emulator..."
$GCLOUD_BIN components install pubsub-emulator --quiet

# Start Pub/Sub emulator
echo "Starting Pub/Sub emulator..."
$GCLOUD_BIN beta emulators pubsub start --host-port=localhost:8085 &
EMULATOR_PID=$!

# Wait for the emulator to start
sleep 5

# Run tests
export PUBSUB_EMULATOR_HOST="localhost:8085"
bazel-bin/your_project/tests/test_binary

# Cleanup
kill $EMULATOR_PID
