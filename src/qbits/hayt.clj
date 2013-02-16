(ns qbits.hayt
  (:require [qbits.hayt.cql :as cql])
  (:import [java.util Date]))

(defn ->cql
  ""
  [query]
  (binding [cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn ->prepared
  ""
  [query]
  (binding [cql/*prepared-statement* true
            cql/*param-stack* (atom [])]
    [(cql/emit-query query)
     @cql/*param-stack*]))

(defn query
  [template query-map]
  (vary-meta query-map assoc :template template))

(defn select
  ""
  [table & clauses]
  (query ["SELECT" :columns "FROM" :table :where :order-by :limit]
         (into {:table table
                :columns []}
               clauses)))

(defn insert
  ""
  [table & clauses]
  (query ["INSERT INTO" :table :values :using]
         (into {:table table}
               clauses)))

(defn update
  ""
  [table & clauses]
  (query ["UPDATE" :table :using :set-columns :where]
         (into {:table table}
               clauses)))

(defn delete
  ""
  [table & clauses]
  (query ["DELETE" :columns "FROM" :table :using :where]
         (into {:table table
                :columns []}
               clauses)))

(defn truncate
  ""
  [table]
  (query ["TRUNCATE" :table]
         {:table table}))

(defn drop-keyspace
  ""
  [keyspace]
  (query ["DROP KEYSPACE" :keyspace]
         {:keyspace keyspace}))

(defn drop-table
  ""
  [table]
  (query ["DROP TABLE" :table]
         {:table table}))

(defn drop-index
  ""
  [index]
  (query ["DROP INDEX" :index]
         {:index index}))

(defn create-index
  ""
  [table index-column & clauses]
  (query ["CREATE INDEX" :index-name "ON" :table :index-column]
         (into {:table table
                :index-column index-column}
               clauses)))

(defn create-keyspace
  ""
  [ks & clauses]
  (query ["CREATE KEYPACE" :keyspace :with]
         (into {:keyspace ks}
               clauses)))

(defn create-table
  [table & clauses]
  (query ["CREATE TABLE" :table :column-definitions :with]
         (into  {:table table}
                clauses)))

(defn alter-table
  "not complete, no tests"
  [table & clauses]
  (query ["ALTER TABLE" :table :table-type :with]
         (into {:table table}
               clauses)))

(defn alter-keyspace
  ""
  [ks & clauses]
  (query ["ALTER KEYPACE" :keyspace :with]
         (into {:keyspace ks}
               clauses)))

(defn batch
  ""
  [& clauses]
  (query ["BATCH" :using :queries "APPLY BATCH"]
         (into {} clauses)))




;; clauses

(defn columns
  ""
  [& columns]
  {:columns columns})

(defn column-definitions
  ""
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  ""
  [& args]
  {:using args})

(defn limit
  ""
  [n]
  {:limit n})

(defn order-by
  ""
  [& columns]
  {:order-by columns})

(defn queries
  ""
  [& queries]
  {:queries queries})

(defn where
  ""
  [args]
  {:where args})

(defn values
  ""
  [values]
  {:values values})

(defn set-columns
  ""
  [values]
  {:set-columns values})

(defn with
  ""
  [values]
  {:with values})

(defn index-name
  ""
  [value]
  {:index-name value})

(defn q->
  ""
  [q & clauses]
  (-> (into q clauses)
      (with-meta (meta q))))

;; CQL3 functions

(def now (constantly (cql/map->CQLFn {:value "now()"})))
(def count* (constantly (cql/map->CQLFn {:value "COUNT(*)"})))
(def count1 (constantly (cql/map->CQLFn {:value "COUNT(1)"})))

(defn date->epoch
  [d]
  (.getTime ^Date d))

(defn max-timeuuid
  ""
  [^Date date]
  (cql/->CQLFn (date->epoch date) "maxTimeuuid(%s)"))

(defn min-timeuuid
  ""
  [^Date date]
  (cql/->CQLFn (date->epoch date) "minTimeuuid(%s)"))

(defn token
  ""
  [token]
  (cql/->CQLFn token "token(%s)"))

(defn writetime
  ""
  [x]
  (cql/->CQLFn x "WRITETIME(%s)"))

(defn ttl
  ""
  [x]
  (cql/->CQLFn x "TTL(%s)"))

(defn unix-timestamp-of
  ""
  [x]
  (cql/->CQLFn x "unixTimestampOf(%s)"))

(defn date-of
  ""
  [x]
  (cql/->CQLFn x "dateOf(%s)"))

(defn blob->type
  ""
  [x]
  (cql/->CQLFn x "blobAsType(%s)"))

(defn type->blob
  ""
  [x]
  (cql/->CQLFn x "typeAsBlob(%s)"))

(defn blob->bigint
  ""
  [x]
  (cql/->CQLFn x "blobAsBigint(%s)"))

(defn bigint->blob
  ""
  [x]
  (cql/->CQLFn x "bigintAsBlob(%s)"))
