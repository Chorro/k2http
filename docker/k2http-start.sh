#!/bin/sh

process_lists () {
	echo
	for tok in $*; do
		# Expand abc:def into abc: def
		local tok=$(echo "$tok" | sed 's%:%: %g')
		echo "    - $tok"
	done
}

if [ "$KAFKA_TOPICS" != "*" ]; then
	export KAFKA_TOPICS=$(process_lists "$KAFKA_TOPICS")
else
	export KAFKA_TOPICS="'*'"
fi
HTTP_POST_PARAMS=$(process_lists "$HTTP_POST_PARAMS")
envsubst < config.yaml_env > config.yaml
exec ./k2http --config config.yaml
