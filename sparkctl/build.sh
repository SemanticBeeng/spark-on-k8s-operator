#!/usr/bin/env bash

SCRIPT=`basename ${BASH_SOURCE[0]}`
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"

set -e

platforms=("linux:amd64" "darwin:amd64")

for platform in "${platforms[@]}"
do

    GOOS="${platform%%:*}"
    GOARCH="${platform#*:}"
    echo $GOOS
    echo $GOARCH
    CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -o sparkctl-${GOOS}-${GOARCH}
done
