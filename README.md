# gus-epaxos

## Testing on a single machine:

`. run.sh` to launch 3 Gus replicas

`bin/client -writes=0.1 -c=-1` to launch clients with 10% writes and Zipfian distribution on keys

`python3 client_metrics.py` to get statistics 

## NOTE
1. There are some operation with large latency. This seems to be some compability issue with the client used.
2. To use this version of client, one needs `git clone golang.org/x/sync`
3. To fetch stats, one needs `numpy` and `statistics`
