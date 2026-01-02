#!/bin/bash

# --- Path and Variable Definitions ---

# etcd version and download source
ETCD_VER="v3.6.6"
DOWNLOAD_BASE_URL="https://storage.googleapis.com/etcd"
DOWNLOAD_FILE="etcd-${ETCD_VER}-linux-amd64.tar.gz"
DOWNLOAD_URL="${DOWNLOAD_BASE_URL}/${ETCD_VER}/${DOWNLOAD_FILE}"
TEMP_DOWNLOAD_PATH="/tmp/${DOWNLOAD_FILE}"

# etcd install location and executable path
ETCD_HOME="$HOME/etcd"
ETCD_BINARY="$ETCD_HOME/etcd"

# Attempt to automatically detect the local IP address.
IP_ADDRESS=$(hostname -I | awk '{print $1}')

# etcd client listen port
CLIENT_PORT="2379"

# --- Script Body ---

# 1. Path and File Existence Check
echo "Checking for etcd executable file at $ETCD_BINARY..."
if [ ! -f "$ETCD_BINARY" ]; then
    echo "Warning: etcd executable not found. Starting automatic download and installation."

    # --- Automatic Download Logic ---
    echo "---"
    echo "Downloading etcd version ${ETCD_VER} from Google Storage..."

    # Check for curl
    if ! command -v curl &> /dev/null
    then
        echo "ERROR: 'curl' is required but not installed. Please install 'curl' and run the script again."
        exit 1
    fi

    # Download the file
    if ! curl -L ${DOWNLOAD_URL} -o ${TEMP_DOWNLOAD_PATH}; then
        echo "ERROR: Download failed. Check network connectivity or the URL."
        exit 1
    fi
    echo "Download successful to ${TEMP_DOWNLOAD_PATH}."

    # Create installation directory
    echo "Creating installation directory: $ETCD_HOME"
    mkdir -p "$ETCD_HOME"

    # Extract the archive
    echo "Extracting files..."
    if ! tar xzvf "${TEMP_DOWNLOAD_PATH}" -C "$ETCD_HOME" --strip-components=1; then
        echo "ERROR: Extraction failed. The downloaded file might be corrupted."
        rm -f "${TEMP_DOWNLOAD_PATH}"
        exit 1
    fi

    # Clean up
    echo "Cleaning up temporary file."
    rm -f "${TEMP_DOWNLOAD_PATH}"

    # Final check after download
    if [ ! -f "$ETCD_BINARY" ]; then
        echo "ERROR: Installation failed. etcd executable still not found after extraction."
        exit 1
    fi
    echo "Installation successful. etcd is ready."
    echo "---"
fi
echo "Check passed: etcd executable found at $ETCD_BINARY."


# 2. IP Address Check
if [ -z "$IP_ADDRESS" ]; then
    echo "ERROR: Could not automatically detect IP address. Please manually edit the IP_ADDRESS variable in the script."
    exit 1
fi

# 3. Construct the full etcd command
FULL_COMMAND="$ETCD_BINARY \
  --listen-client-urls http://0.0.0.0:$CLIENT_PORT \
  --advertise-client-urls http://$IP_ADDRESS:$CLIENT_PORT"

# 4. Print the command to be executed
echo "---"
echo "Starting etcd with the following IP address: $IP_ADDRESS"
echo "The full etcd startup command is:"
echo "$FULL_COMMAND"
echo "---"

# 5. Execute the command
echo "Starting etcd..."
exec $FULL_COMMAND