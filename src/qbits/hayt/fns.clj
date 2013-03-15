(ns qbits.hayt.fns
  (:require [qbits.hayt.cql :as cql])
  (:import [java.util Date]))

(def now
  "Returns a now() CQL function"
  (constantly (cql/cql-fn "now")))

(def count*
  "Returns a count(*) CQL function"
  (constantly (cql/cql-fn "COUNT" :*)))

(def count1
  "Returns a count(1) CQL function"
  (constantly (cql/cql-fn "COUNT" 1)))

(defn date->epoch
  [d]
  (.getTime ^Date d))

(defn max-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql/cql-fn "maxTimeuuid" (date->epoch date)))

(defn min-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql/cql-fn "minTimeuuid" (date->epoch date)))

(defn token
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a token function with the supplied argument"
  [token]
  (cql/cql-fn "token" token))

(defn writetime
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a WRITETIME function with the supplied argument"
  [x]
  (cql/cql-fn "WRITETIME" x))

(defn ttl
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a TTL function with the supplied argument"
  [x]
  (cql/cql-fn "TTL" x))

(defn unix-timestamp-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a unixTimestampOf function with the supplied argument"
  [x]
  (cql/cql-fn "unixTimestampOf" x))

(defn date-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a dateOf function with the supplied argument"
  [x]
  (cql/cql-fn "dateOf" x))

(defn blob->type
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/cql-fn "blobAsType" x))

(defn type->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/cql-fn "typeAsBlob" x))

(defn blob->bigint
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/cql-fn "blobAsBigint" x))

(defn bigint->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/cql-fn "bigintAsBlob" x))
