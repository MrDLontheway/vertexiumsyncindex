#!/usr/bin/env bash

jar -vtf /opt/cloudera/parcels/CDH/lib/hbase/lib/phoenix-4.8.0-cdh5.8.0-server.jar | grep 'org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec'
