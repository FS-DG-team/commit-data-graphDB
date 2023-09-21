#! /bin/bash

# start sshd 
[ -f /home/tigergraph/.ssh/sshd_rsa ] || ssh-keygen -b 4096 -t rsa -f /home/tigergraph/.ssh/sshd_rsa -q -N ''
if [ ! -f /home/tigergraph/.ssh/tigergraph_rsa ]; then
  ssh-keygen -b 4096 -t rsa -f /home/tigergraph/.ssh/tigergraph_rsa -q -N ''
  cat /home/tigergraph/.ssh/tigergraph_rsa.pub > /home/tigergraph/.ssh/authorized_keys
fi

# start tigergraph
sudo /usr/sbin/sshd -h /home/tigergraph/.ssh/sshd_rsa
/home/tigergraph/tigergraph/app/cmd/gadmin start all --auto-restart

# init schemas  ( https://towardsdatascience.com/efficient-use-of-tigergraph-and-docker-5e7f9918bf53 )
if [ -n "$(ls -A /home/tigergraph/graph-schemas/ 2>/dev/null)" ]; then
    for file in /home/tigergraph/graph-schemas/*.gsql; do 
        /home/tigergraph/tigergraph/app/cmd/gsql -f $file
    done 
fi

# tail logs
tail -f /home/tigergraph/tigergraph/log/admin/ADMIN.INFO
