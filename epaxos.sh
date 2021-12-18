bin/master -N 3 &
sleep 0.1
#bin/server -maddr 10.142.0.74 -addr 10.142.0.74 -e=true &
bin/server -port 7070 -gus=false -e=true -exec=true &
sleep 0.1
bin/server -port 7071 -gus=false -e=true -exec=true &
sleep 0.1
bin/server -port 7072 -gus=false -e=true -exec=true &
