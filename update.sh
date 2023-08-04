. killall.sh
cd ../gus-epaxos || exit
git stash && git stash clear && git pull

go install master
go install server
go install client