(ns qbits.hayt
  (:require [qbits.hayt.cql :as cql])
  (:import [java.util Date]))

(defn ->raw
  "Compiles a hayt query into its raw/string value"
  [query]
  (binding [cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn ->prepared
  "Compiles a hayt query into a vector composed of the prepared string
  query and a vector of parameters."
  [query]
  (binding [cql/*prepared-statement* true
            cql/*param-stack* (atom [])]
    [(cql/emit-query query)
     @cql/*param-stack*]))

(defn query
  [template query-map]
  (vary-meta query-map assoc :template template))

(defn select
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* where
* order-by
* limit)"
  [table & clauses]
  (query ["SELECT" :columns "FROM" :table :where :order-by :limit]
         (into {:table table :columns []} clauses)))

(defn insert
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:

* values
* using"
  [table & clauses]
  (query ["INSERT INTO" :table :values :using]
         (into {:table table}  clauses)))

(defn update
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where"
  [table & clauses]
  (query ["UPDATE" :table :using :set-columns :where]
         (into {:table table}  clauses)))

(defn delete
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where"
  [table & clauses]
  (query ["DELETE" :columns "FROM" :table :using :where]
         (into {:table table :columns []} clauses)))

(defn truncate
  "http://cassandra.apache.org/doc/cql3/CQL.html#truncateStmt

Takes a table identifier."
  [table]
  (query ["TRUNCATE" :table]
         {:table table}))

(defn drop-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropKeyspaceStmt

Takes a keyspace identifier"
  [keyspace]
  (query ["DROP KEYSPACE" :keyspace]
         {:keyspace keyspace}))

(defn drop-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropTableStmt

Takes a table identifier"
  [table]
  (query ["DROP TABLE" :table]
         {:table table}))

(defn drop-index
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropIndexStmt

Takes an index identifier."
  [index]
  (query ["DROP INDEX" :index]
         {:index index}))

(defn create-index
  "http://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt

Takes a table identifier and additional clause arguments:

* index-column
* index-name"
  [table index-column & clauses]
  (query ["CREATE INDEX" :index-name "ON" :table :index-column]
         (into {:table table :index-column index-column} clauses)))

(defn create-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and a with clause.
"
  [keyspace & clauses]
  (query ["CREATE KEYSPACE" :keyspace :with]
         (into {:keyspace keyspace} clauses)))

(defn create-table
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with"
  [table & clauses]
  (query ["CREATE TABLE" :table :column-definitions :with]
         (into {:table table} clauses)))

(defn alter-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add
* with"
  [table & clauses]
  (query ["ALTER TABLE" :table :alter-column :add :with]
         (into {:table table} clauses)))

(defn alter-column-family
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a column-familiy identifier and additional clause arguments:

* alter-column
* add
* with"
  [column-family & clauses]
  (query ["ALTER COLUMNFAMILY" :column-family :alter-column :add :with]
         (into {:column-family column-family} clauses)))

(defn alter-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause."
  [keyspace & clauses]
  (query ["ALTER KEYSPACE" :keyspace :with]
         (into {:keyspace keyspace} clauses)))

(defn batch
  "http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt

Takes a list (vararg) of hayt queries and an optional `using` clause."
  [& clauses]
  (query ["BATCH" :using :queries "APPLY BATCH"]
         (into {} clauses)))

(defn use-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  (query ["USE" :keyspace]
         {:keyspace keyspace}))

;; Clauses

(defn columns
  "Taks a list (vararg) of columns identifiers"
  [& columns]
  {:columns columns})

(defn column-definitions
  ""
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  "Takes keyword/value pairs for :timestamp and :ttl"
  [& args]
  {:using args})

(defn limit
  "Takes a numeric value"
  [n]
  {:limit n})

(defn order-by
  "Takes vectors of 2 elements, where the first is the column
  identifier and the second is the ordering (as a keyword,
  ex: :asc, :desc)"
  [& columns] {:order-by columns})
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

(defn alter-column
  ""
  [& args]
  {:alter-column args})

(defn add
  ""
  [& args]
  {:add args})

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


;; Sugar for collection types

(defn coll-type
  [t & spec]
  (format "%s<%s>"
          (name t)
          (cql/join-comma (map name spec))))

(def map-type (partial coll-type :map))
(def list-type (partial coll-type :list))
(def set-type (partial coll-type :set))

;; Utilities

(defn apply-map
  "Takes a generated prepared query with its arg vector containing
  keywords for placeholders and maps the supplied map to it"
  [[query placeholders] parameter-map]
  [query (replace parameter-map placeholders)])
