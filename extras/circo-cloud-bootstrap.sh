#!/bin/bash
set -e
set -u

#
# Make sure that the AWS credentials are defined by the following variables
# - AWS_ACCESS_KEY
# - AWS_SECRET_KEY
#

CIRCO_TAR="http://cbcrg-eu.s3.amazonaws.com/circo-0.1.tar"
CIRCO_CLUSTER=$1


#
# Download the tar file
#
wget -q -O tmp.tgz http://cbcrg-eu.s3.amazonaws.com/circo-0.1.tgz
tar xvf tmp.tgz
rm -rf tmp.tgz
cd circo-

#
# List all IP addresses of running instances tagged with the specified cluster name
#
ec2-describe-instances -F instance-state-name=running -F "tag:circo-cluster=$CIRCO_CLUSTER" --hide-tags | grep ^INSTANCE | cut -f 18 > addresses

if [ -s addresses ]; then
    # when there is at least and address the cluster already exist, join to that machine(s)
    cmdline="./bin/circo-daemon.sh --join @addresses"
else
    # when the 'addresses' file is empty this is the first node in the cluster
    # so do NOT join to anybody
    cmdline="./bin/circo-daemon.sh"
fi

#
# Launch the node
#
"nohup $cmdline > /dev/null &"

#
# Tag this node, to mark it as included in the cluster
#
instance=$(wget -q -O - http://169.254.169.254/latest/meta-data/instance-id)
ec2-create-tags $instance --tag "circo-cluster=$CIRCO_CLUSTER"

