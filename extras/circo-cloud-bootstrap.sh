#!/bin/bash
set -e
set -u
env | sort
echo === begin circo bootstrap ===
#
# Make sure that the AWS credentials are defined by the following variables
# - CIRCO_CLUSTER
# - AWS_ACCESS_KEY
# - AWS_SECRET_KEY
#
#
# To use, include in the EC2 user-data field the following snippet
#   https://gist.github.com/4595210
#

CIRCO_CLUSTER=${CIRCO_CLUSTER:-circotest1}
CIRCO_NAME=${CIRCO_NAME:-circo-0.2.2}
CIRCO_TAR=${CIRCO_TAR:-http://cbcrg-eu.s3.amazonaws.com/$CIRCO_NAME.tgz}
CIRCO_PORT=${CIRCO_PORT:-2551}
CIRCO_USER=${CIRCO_USER:-$USER}
CIRCO_HOME=${CIRCO_HOME:-$(eval echo ~${CIRCO_USER}/$CIRCO_NAME)}
EC2_HOME=${EC2_HOME:-/opt/aws/apitools/ec2}
export EC2_HOME

set +u
if [ -z $JAVA_HOME ]; then
JAVA_HOME=$(readlink -f `which java`) && JAVA_HOME=$(dirname $JAVA_HOME) && JAVA_HOME=$(dirname $JAVA_HOME)
export JAVA_HOME
fi
set -u

# PATH
if ! command -v ec2-describe-instances &>/dev/null; then
PATH+=":$EC2_HOME/bin"
fi

#
# Download the tar file
#
rm -rf $CIRCO_HOME
wget -q -O tmp.tgz $CIRCO_TAR
tar xvf tmp.tgz
rm -rf tmp.tgz
mv circo-* $CIRCO_HOME
chown -R $CIRCO_USER $CIRCO_HOME
cd $CIRCO_HOME

#
# List all IP addresses of running instances tagged with the specified cluster name
#
zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)
region="${zone%?}"
ec2-describe-instances --region $region -F instance-state-name=running -F "tag:circo-cluster=$CIRCO_CLUSTER" --hide-tags | grep ^INSTANCE | cut -f 18 > addresses

HOSTIP=$(wget -q -O - http://169.254.169.254/latest/meta-data/local-ipv4)
cmdline="./bin/circo-daemon.sh --host $HOSTIP --port $CIRCO_PORT"

if [ -s addresses ]; then
    # when there is at least and address the cluster already exist, join to that machine(s)
    cmdline+=" --join @./addresses"
fi

#
# Launch the node
#
nohup $cmdline &> /dev/null &

#
# Tag this node, to mark it as included in the cluster
#
instance=$(wget -q -O - http://169.254.169.254/latest/meta-data/instance-id)
ec2-create-tags $instance --tag "circo-cluster=$CIRCO_CLUSTER" --region $region

