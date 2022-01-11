#!/bin/sh

# Instructions: Cloudlab doesn’t store ssh keys in its disk images, so we have to manually add rabia’s public key into each machine’s authorized_keys file. 
# Change the MACHINE_ADDRESSES and USERNAME variable, then run this script from your local machine that has a cloudlab ssh private key to enable connecting to 
# the machines via root. 

# Note: supported formats for machine addresses are ip addresses or urls seperated by spaces
# 	example 1: ms1203.utah.cloudlab.us
# 	example 2: 128.110.217.208
MACHINE_ADDRESSES="ms1203.utah.cloudlab.us ms1234.utah.cloudlab.us ms1235.utah.cloudlab.us ms1215.utah.cloudlab.us"
USERNAME="zhouaea"

for MACHINE_ADDRESS in ${MACHINE_ADDRESSES}
do
	echo "${USERNAME}@${MACHINE_ADDRESS}"
	ssh -t "${USERNAME}@${MACHINE_ADDRESS}" "sudo su -c \"chmod 644 /root/.ssh/authorized_keys; cat /root/go/src/rabia/deployment/install/id_rsa.pub >> /root/.ssh/authorized_keys\""
done
