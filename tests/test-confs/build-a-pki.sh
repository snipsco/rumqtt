#!/bin/sh

set -xe
rm -rf *.cert *.key *.req

PATH=/usr/local/Cellar/openssl/1.0.2n/bin/:$PATH

# CA key + cert
openssl req -nodes \
          -x509 \
          -days 3650 \
          -newkey rsa:2048 \
          -keyout ca.key \
          -out ca.cert \
          -sha256 \
          -batch \
          -subj "/CN=ponytown RSA CA"

# server key + cert req
openssl req -nodes \
          -newkey rsa:2048 \
          -keyout server.key \
          -out server.req \
          -sha256 \
          -batch \
          -subj "/CN=ponytown server"

# client key + cert req
openssl req -nodes \
          -newkey rsa:2048 \
          -keyout client.key \
          -out client.req \
          -sha256 \
          -batch \
          -subj "/CN=ponytown client"

# CA signs server certificate request
openssl x509 -req \
        -in server.req \
        -out server.cert \
        -CA ca.cert \
        -CAkey ca.key \
        -sha256 \
        -days 2000 \
        -set_serial 456 \
        -extensions v3_end -extfile openssl.cnf

# CA signs client certificate request
openssl x509 -req \
        -in client.req \
        -out client.cert \
        -CA ca.cert \
        -CAkey ca.key \
        -sha256 \
        -days 2000 \
        -set_serial 789 \
        -extensions v3_client -extfile openssl.cnf

cat server.cert ca.cert > server.chain
cat client.cert ca.cert > client.chain
