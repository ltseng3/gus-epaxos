sudo tc qdisc del dev eno1d1 root
sudo tc qdisc add dev eno1d1 root handle 1: htb
sudo tc class add dev eno1d1 parent 1: classid 1:1 htb rate 1gibps
sudo tc class add dev eno1d1 parent 1:1 classid 1:2 htb rate 1gibps
sudo tc qdisc add dev eno1d1 handle 2: parent 1:2 netem delay 36ms
sudo tc filter add dev  eno1d1 pref 2 protocol ip u32 match ip dst 10.10.1.1 flowid 1:2
sudo tc class add dev eno1d1 parent 1:1 classid 1:3 htb rate 1gibps
sudo tc qdisc add dev eno1d1 handle 3: parent 1:3 netem delay 44ms
sudo tc filter add dev eno1d1 pref 3 protocol ip u32 match ip dst 10.10.1.3 flowid 1:3