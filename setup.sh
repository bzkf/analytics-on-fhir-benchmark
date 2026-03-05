#!/bin/bash

sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin

# Add Docker's official GPG key:
# apt-get update
# apt-get install -y ca-certificates
# install -m 0755 -d /etc/apt/keyrings
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
# chmod a+r /etc/apt/keyrings/docker.asc

# # Add the repository to Apt sources:
# echo \
#   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
#   $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
#    tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y python3-pip python3.12-venv openjdk-17-jre-headless

# use an older version as cadvisor isn't compatible yet with Docker 29+
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh --version 28.5.2

python3 -m venv .venv
