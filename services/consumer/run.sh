#!/bin/bash
set -e

# 1. Inject environment variables
sed -i "s|%KINESIS_STREAM%|$KINESIS_STREAM|g" kcl.properties
sed -i "s|%AWS_REGION%|$AWS_REGION|g" kcl.properties
sed -i "s|%APPLICATION_NAME%|${APPLICATION_NAME:-ConsumerApp}|g" kcl.properties

# 2. Construct the Java command manually
echo "Locating KCL JARs..."
KCLPY_PATH=$(python -c "import os, amazon_kclpy; print(os.path.dirname(amazon_kclpy.__file__))")

if [ -z "$KCLPY_PATH" ]; then
    echo "Error: Could not find amazon_kclpy package."
    exit 1
fi

JARS_DIR="$KCLPY_PATH/jars"

# Build CLASSPATH by joining all .jar files with ':'
# This avoids shell expansion issues and ignores non-jar files (like __pycache__)
CLASSPATH=$(find "$JARS_DIR" -name "*.jar" | tr '\n' ':')

echo "Classpath length: ${#CLASSPATH}"

# 3. Execute the MultiLangDaemon directly
CMD="java -cp $CLASSPATH software.amazon.kinesis.multilang.MultiLangDaemon --properties-file kcl.properties"

echo "Starting KCL Daemon..."
exec $CMD
