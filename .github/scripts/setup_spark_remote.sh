#!/usr/bin/env bash

set -xve
echo "Setting up spark-connect"

mkdir -p "$HOME"/spark
cd "$HOME"/spark || exit 1
spark_versions=$(wget -qO - https://dlcdn.apache.org/spark/ | grep 'href="spark-[0-9.]*\/"' | sed 's:</a>:\n:g' | sed -n 's/.*>//p' | tr -d spark/- | sort -rV)
echo "Available Spark versions:" $spark_versions

desired_version="3.5"
matching_version=$(echo "$spark_versions" | grep "^${desired_version}\." | head -1)

if [ -n "$matching_version" ]; then
    version=$matching_version
else
    version=$(echo "$spark_versions" | head -1)
fi

if [ -z "$version" ]; then
  echo "Failed to extract Spark version"
   exit 1
fi
echo "Loading spark version $version"

spark=spark-${version}-bin-hadoop3
spark_connect="spark-connect_2.12"

mkdir -p "${spark}"


SERVER_SCRIPT=$HOME/spark/${spark}/sbin/start-connect-server.sh

## check the spark version already exist, if not download the respective version
if [ -f "${SERVER_SCRIPT}" ];then
  echo "Spark Version already exists"
else
  if [ -f "${spark}.tgz" ];then
    echo "${spark}.tgz already exists"
  else
    wget "https://dlcdn.apache.org/spark/spark-${version}/${spark}.tgz"
  fi
  tar -xvf "${spark}.tgz"
fi

cd "${spark}" || exit 1
## check spark remote is running,if not start the spark remote
result=$(${SERVER_SCRIPT} --packages org.apache.spark:${spark_connect}:"${version}" > "$HOME"/spark/log.out; echo $?)

if [ "$result" -ne 0 ]; then
    count=$(tail "${HOME}"/spark/log.out | grep -c "SparkConnectServer running as process")
    if [ "${count}" == "0" ]; then
            echo "Failed to start the server"
        exit 1
    fi
    # Wait for the server to start by pinging localhost:4040
    echo "Waiting for the server to start..."
    for i in {1..30}; do
        if nc -z localhost 4040; then
            echo "Server is up and running"
            break
        fi
        echo "Server not yet available, retrying in 5 seconds..."
        sleep 5
    done

    if ! nc -z localhost 4040; then
        echo "Failed to start the server within the expected time"
        exit 1
    fi
fi
echo "Started the Server"
