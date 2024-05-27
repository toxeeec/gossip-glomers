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
*)
	>&2 echo "Unknown challenge id: $2"
	exit 1
	;;
esac

eval "go build cmd/$2*/main.go && $1 test -w $workload --bin main $args"
