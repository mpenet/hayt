(ns qbits.hayt.fns
  (:require
   [clojure.string :as string]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.utils :as u])
  (:import (java.util Date)))

(defn cql-raw
  "Allows to pass raw (assumed safe) content, no escaping will be
applied. `preparable?` allows to control how the value will be handled
when the query is compiled as a prepared statement"
  ([x preparable?]
     (if preparable?
       (cql/->CQLRawPreparable x)
       (cql/->CQLRaw x)))
  ([x]
     (cql-raw x false)))

(defn cql-fn
  "Calls supplied function by name, with the supplied args"
  [name & args]
  (cql/->CQLFn name args))

(defn as
  "Aliases a column (selector) to another identifier (id)"
  [selector id]
  (cql/->CQLRaw (str (cql/cql-identifier selector)
                     " AS "
                     (cql/cql-identifier id))))

(def now
  "Returns a now() CQL function"
  (constantly (cql-fn "now")))

(def count*
  "Returns a count(*) CQL function"
  (constantly (cql-fn "COUNT" :*)))

(def count1
  "Returns a count(1) CQL function"
  (constantly (cql-fn "COUNT" 1)))

(defn date->epoch
  [d]
  (.getTime ^Date d))

(defn max-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql-fn "maxTimeuuid" (date->epoch date)))

(defn min-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql-fn "minTimeuuid" (date->epoch date)))

(defn token
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a token function with the supplied argument"
  [& tokens]
  (apply cql-fn "token" tokens))

(defn writetime
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a WRITETIME function with the supplied argument"
  [x]
  (cql-fn "WRITETIME" x))

(defn ttl
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a TTL function with the supplied argument"
  [x]
  (cql-fn "TTL" x))

(defn unix-timestamp-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a unixTimestampOf function with the supplied argument"
  [x]
  (cql-fn "unixTimestampOf" x))

(defn date-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a dateOf function with the supplied argument"
  [x]
  (cql-fn "dateOf" x))

(defn distinct*
  "Returns DISTINCT column id ex: `(select :table (columns (distinct :foo)))`"
  [x]
  (cql-raw (str "DISTINCT " (cql/cql-identifier x))))

(def ?
  "? can be used as a query value to mark a prepared statement value
ex:    (select :foo
           (where [[:foo  [> ?]]
                   [:foo  [< 2]]]))"
  (cql-raw "?"))

;; blob convertion fns
;;

(defmacro ^:no-doc gen-blob-fns
  "Generator for blobAs[Type] and [Type]asBlob functions"
  []
  `(do
    ~@(for [t (remove #{:blob} u/native-types)
            :let [t (name t)]]
        `(do
           (defn ~(symbol (str "blob->" t))
             ~(format "Converts blob to %s.
See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions"
                      t)
             [x#]
             (cql-fn ~(str "blobAs" (string/capitalize t)) x#))
           (defn ~(symbol (str t "->blob"))
             ~(format "Converts %s to blob.
See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions"
                      t)
             [x#]
             (cql-fn ~(str t "AsBlob") x#))))))

(gen-blob-fns)
