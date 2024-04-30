export GOPATH=/root/go/
export GOBIN=/root/go/src/gus-epaxos/bin
go install gus-epaxos/src/master
go install gus-epaxos/src/server
go install gus-epaxos/src/client
go install gus-epaxos/src/clientepaxos
go install gus-epaxos/src/clientpaxos
export GOBIN=
