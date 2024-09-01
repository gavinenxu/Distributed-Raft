# Distributed-Raft

## Assume you're in raft directory

## How to use dslogs to analyze log 
```
VERBOSE=1 go test -run TestInitialElection | ../tools/dslogs -c 3 -i PERS
```

## How to run multiple servers (30) with multiple times (100)
```
../tools/dstest PartB -p 30 -n 100  
```
