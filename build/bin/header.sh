#!/bin/bash
# Kyligence Inc. License

# source me

verbose=${verbose:-""}

while getopts ":v" opt; do
    case $opt in
        v)
            echo "Turn on verbose mode." >&2
            verbose=true
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            ;;
    esac
done

if [[ "$dir" == "" ]]
then
	dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
	
	# set KYLIN_HOME with consideration for multiple instances that are on the same node
	KYLIN_HOME=${KYLIN_HOME:-"${dir}/../"}
	export KYLIN_HOME=`cd "$KYLIN_HOME"; pwd`
	dir="$KYLIN_HOME/bin"
	
	function quit {
		echo "$@"
		exit 1
	}
	
	function verbose {
		if [[ -n "$verbose" ]]; then
			echo "$@"
		fi
	}
fi
