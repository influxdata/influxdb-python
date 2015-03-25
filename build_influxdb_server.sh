#!/usr/bin/env bash

#
# build and install,
# the latest influxdb server master
#

set -e

tmpdir=$(mktemp -d)

echo "Using tempdir $tmpdir .."

cd "$tmpdir"

# rpm for package.sh (below) which will also build an .rpm
sudo apt-get install ruby ruby-dev build-essential rpm

echo $PATH
echo $(which gem)
echo $(which ruby)

gem=$(which gem)

sudo $gem install fpm

mkdir -p go/src/github.com/influxdb
cd go/src/github.com/influxdb

git clone --depth 5 https://github.com/influxdb/influxdb
cd influxdb

version=0.0.0-$(git describe --always | sed 's/^v//')
echo "describe: $version"

export GOPATH="$tmpdir/go"
{ echo y ; yes no ; } | ./package.sh "$version"

deb=$(ls *.deb)
sudo dpkg -i "$deb"
