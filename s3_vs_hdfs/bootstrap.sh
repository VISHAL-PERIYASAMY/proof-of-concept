#!/bin/bash
aws redshift describe-clusters --cluster-identifier redshift-cluster  | jq -r '.Clusters[0].ClusterPublicKey' >> /home/hadoop/.ssh/authorized_keys

set -e
cur_dir=`pwd`
pwd
echo -e "Installing Python dependencies:\n"
sudo echo -e "boto3==1.17.35\nwheel==0.36.2\nSQLAlchemy==1.4.13\npsycopg2-binary==2.8.6" >> requirements.txt
sudo cat requirements.txt
sudo easy_install-3.7 pip
sudo /usr/local/bin/pip3 install -r requirements.txt --target=/usr/local/lib/python3.7/site-packages