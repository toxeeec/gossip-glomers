#!/bin/bash

if [ "$#" -ne 2 ]; then
	>&2 echo "Usage: $(basename "$0") maelstrom_path challenge_id"
	exit 1
fi

case "$2" in
"1")
	workload="echo"
	args="--node-count 1 --time-limit 10"
	;;
"2")
	workload="unique-ids"
	args="--time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition"
	;;
"3a")
	workload="broadcast"
	args="--node-count 1 --time-limit 20 --rate 10"
	;;
"3b")
	workload="broadcast"
	args="--node-count 5 --time-limit 20 --rate 10"
	;;
"3c")
	workload="broadcast"
	args="--node-count 5 --time-limit 20 --rate 10 --nemesis partition"
	;;
"3d" | "3e")
	workload="broadcast"
	args="--node-count 25 --time-limit 20 --rate 100 --latency 100"
	;;
"4")
	workload="g-counter"
	args="--node-count 3 --rate 100 --time-limit 20 --nemesis partition"
	;;
"5a")
	workload="kafka"
	args="--node-count 1 --concurrency 2n --time-limit 20 --rate 1000"
	;;
"5b" | "5c")
	workload="kafka"
	args="--node-count 2 --concurrency 2n --time-limit 20 --rate 1000"
	;;
"6a")
	workload="txn-rw-register"
	args="--node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total"
	;;
*)
	>&2 echo "Unknown challenge id: $2"
	exit 1
	;;
esac

eval "go build cmd/$2*/main.go && $1 test -w $workload --bin main $args"
