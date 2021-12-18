# gus-epaxos
Testing on a single machine:

`. run.sh` to launch 3 Gus replicas

`bin/client -writes=0.1 -c=-1` to launch clients with 10% writes and Zipfian distribution on keys

`python3 client_metrics.py` to get statistics 
NOTE: there are some operation with large latency. This seems to be some compability issue with the client used.
