#!/usr/bin/env bash

dbname=$1

# compile VIC
scriptpath=$(dirname "$(realpath "$0")")
curdir=$(pwd)
cd "${scriptpath}"/../external/vic/src || exit
make clean && make
cd "${curdir}" || return

# set up database
[ -z "${dbname}" ] && dbname=rheas

createdb "${dbname}"
echo "CREATE EXTENSION postgis;CREATE EXTENSION postgis_topology;" | psql -d "${dbname}"

# restore database dump
datapath="${scriptpath}"/../data
pd_restore -d "${dbname}" -C -Fd "${datapath}"/rheas.db

# uncompress VIC input files
cd "${datapath}"/vic || exit
for f in *.gz; do
    gunzip "${f}"
done
cd "${curdir}" || return
