#!/bin/sh
set -e

COMPOSEFILE="${0%/*}/../docker-compose-server.yml"
echo $SCRIPTDIR

docker-compose -f "$COMPOSEFILE" build --force-rm
docker-compose -f "$COMPOSEFILE" push
