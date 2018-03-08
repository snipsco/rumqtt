#!/bin/sh


PATH=$PATH:/usr/local/sbin

for mos in "" -tls -tls-pwd -tls-cert
do
  mosquitto -v -c tests/test-confs/mosquitto$mos.conf 2>&1 | sed -e "s/^/[mosquitto$mos] /" &
done

function finish {
    kill $(jobs -p)
}
trap finish EXIT

sleep 2

set -ex
cargo test --verbose --all --features local-tests
