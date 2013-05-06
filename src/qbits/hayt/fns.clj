(ns qbits.hayt.fns
  (:require
   [clojure.string :as string]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.utils :as u])
  (:import (java.util Date)))

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

;; blob convertion fns
;;

(defmacro gen-blob-fns
  "Generator for blobAs[Type] and [Type]asBlob functions"
  []
  `(do
    ~@(for [t u/native-types
            :let [t (name t)]]
        `(do
           (defn ~(symbol (str "blob->" t))
             ~(format "Converts blob to %s.
See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions"
                      t)
             [x#]
             (cql/cql-fn ~(str "blobAs" (string/capitalize t)) x#))
           (defn ~(symbol (str t "->blob"))
             ~(format "Converts %s to blob.
See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions"
                      t)
             [x#]
             (cql/cql-fn ~(str t "AsBlob") x#))))))

(gen-blob-fns)
