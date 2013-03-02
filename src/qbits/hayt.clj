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
* limit
* table (optionaly using composition)"
  [table & clauses]
  (query ["SELECT" :columns "FROM" :table :where :order-by :limit
          :allow-filtering]
         (into {:table table :columns []} clauses)))

(defn insert
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using
* table (optionaly using composition)"
  [table & clauses]
  (query ["INSERT INTO" :table :values :using]
         (into {:table table}  clauses)))

(defn update  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* table (optionaly using composition)"
  [table & clauses]
  (query ["UPDATE" :table :using :set-columns :where]
         (into {:table table}  clauses)))

(defn delete
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* table (optionaly using composition)"
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
* index-name
* table (optionaly using composition)"
  [table index-column & clauses]
  (query ["CREATE INDEX" :index-name "ON" :table :index-column]
         (into {:table table :index-column index-column} clauses)))

(defn create-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (query ["CREATE KEYSPACE" :keyspace :with]
         (into {:keyspace keyspace} clauses)))

(defn create-table
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with
* table (optionaly using composition)"
  [table & clauses]
  (query ["CREATE TABLE" :table :column-definitions :with]
         (into {:table table} clauses)))

(defn alter-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename
* table (optionaly using composition)"
  [table & clauses]
  (query ["ALTER TABLE" :table
          :alter-column :add-column :rename-column :with]
         (into {:table table} clauses)))

(defn alter-column-family
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a column-familiy identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename
* column-family (optionaly using composition)"
  [column-family & clauses]
  (query ["ALTER COLUMNFAMILY" :column-family
          :alter-column :add-column :rename-column :with]
         (into {:column-family column-family} clauses)))

(defn alter-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause.
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (query ["ALTER KEYSPACE" :keyspace :with]
         (into {:keyspace keyspace} clauses)))

(defn batch
  "http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt

Takes hayt queries  optional clauses:
* using
* counter
* logged "
  [& clauses]
  (query [:begin-batch :using :queries "APPLY BATCH"]
         (into {:begin-batch {} :logged true} clauses)))

(defn use-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  (query ["USE" :keyspace]
         {:keyspace keyspace}))

(defn grant-user
  "Takes clauses:
* permission
* resource
* user (optionaly using composition)"
  [user & clauses]
  (query ["GRANT" :permission :resource "TO" :user]
         (into {:user user} clauses)))

(defn revoke-user
  "Takes clauses:
* permission
* resource
* user (optionaly using composition)"
  [user & clauses]
  (query ["REVOKE" :permission :resource "FROM" :user]
         (into {:user user} clauses)))

(defn create-user
  "Takes clauses:
* password
* superuser (defaults to false)
* user (optionaly using composition)"
  [user & clauses]
  (query ["CREATE USER" :user :password :superuser]
         (into {:user user :superuser false} clauses)))

(defn alter-user
  "Takes clauses:
* password
* superuser (defaults to false)
* user (optionaly using composition)"
  [user & clauses]
  (query ["ALTER USER" :user :password :superuser]
         (into {:user user :superuser false} clauses)))

(defn drop-user
  [user]
  (query ["DROP USER" :user]
         {:user user}))

(defn list-users
  []
  (query ["LIST USERS"] {}))

(defn list-permissions
  "Takes clauses:
* permissions (defaults to ALL if not supplied)
* user
* resource
* recursive (defaults to true)"
  [& clauses]
  (query ["LIST" :permission :resource "OF" :user :recursive]
         (into {:permission :ALL :recursive true} clauses)))

;; Clauses

(defn table
  "Clause: takes a table identifier"
  [table]
  {:table table})

(defn keyspace
  "Clause: takes a keyspace identifier"
  [keyspace]
  {:keyspace keyspace})

(defn column-family
  "Clause: takes a column family identifier"
  [keyspace]
  {:column-family column-family})

(defn columns
  "Clause: takes columns identifiers"
  [& columns]
  {:columns columns})

(defn column-definitions
  "Clause: "
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  "Clause: takes keyword/value pairs for :timestamp and :ttl"
  [& args]
  {:using args})

(defn limit
  "Clause: takes a numeric value"
  [n]
  {:limit n})

(defn order-by
  "Clause: takes vectors of 2 elements, where the first is the column
  identifier and the second is the ordering as keyword.
  ex: :asc, :desc"
  [& columns] {:order-by columns})

(defn queries
  "Clause: takes hayt queries to be executed during a batch operation."
  [& queries]
  {:queries queries})

(defn where
  "Clause: takes a map or a vector of pairs to compose the where
clause of a select/update/delete query"
  [args]
  {:where args})

(defn values
  "Clause: "
  [values]
  {:values values})

(defn set-columns
  "Clause: "
  [values]
  {:set-columns values})

(defn with
  "Clause: "
  [values]
  {:with values})

(defn index-name
  "Clause: "
  [value]
  {:index-name value})

(defn alter-column
  "Clause: "
  [& args]
  {:alter-column args})

(defn add-column
  "Clause: "
  [& args]
  {:add-column args})

(defn rename-column
  "Clause: "
  [& args]
  {:rename-column args})

(defn allow-filtering
  "Clause: "
  [value]
  {:allow-filtering value})

(defn logged
  "Clause: "
  [value]
  {:logged value})

(defn counter
  "Clause: "
  [value]
  {:counter value})

(defn resource
  "Clause: "
  [value]
  {:resource value})

(defn user
  "Clause: "
  [value]
  {:user value})

(defn superuser
  "Clause: "
  [value]
  {:superuser value})

(defn password
  "Clause: "
  [value]
  {:password value})

(defn permission
  "Clause: "
  [value]
  {:permission value})

(defn recursive
  "Clause: "
  [value]
  {:recursive value})



(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (-> (into q clauses)
      (with-meta (meta q))))

;; CQL3 functions

(def ^{:doc "Returns a now() CQL function"} now
  (constantly (cql/map->CQLFn {:value "now()"})))

(def ^{:doc "Returns a count(*) CQL function"} count*
  (constantly (cql/map->CQLFn {:value "COUNT(*)"})))

(def ^{:doc "Returns a count(1) CQL function"} count1
  (constantly (cql/map->CQLFn {:value "COUNT(1)"})))

(defn date->epoch
  [d]
  (.getTime ^Date d))

(defn max-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql/->CQLFn (date->epoch date) "maxTimeuuid(%s)"))

(defn min-timeuuid
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid"
  [^Date date]
  (cql/->CQLFn (date->epoch date) "minTimeuuid(%s)"))

(defn token
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a token function with the supplied argument"
  [token]
  (cql/->CQLFn token "token(%s)"))

(defn writetime
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a WRITETIME function with the supplied argument"
  [x]
  (cql/->CQLFn x "WRITETIME(%s)"))

(defn ttl
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Returns a TTL function with the supplied argument"
  [x]
  (cql/->CQLFn x "TTL(%s)"))

(defn unix-timestamp-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a unixTimestampOf function with the supplied argument"
  [x]
  (cql/->CQLFn x "unixTimestampOf(%s)"))

(defn date-of
  "http://cassandra.apache.org/doc/cql3/CQL.html#usingtimeuuid

Returns a dateOf function with the supplied argument"
  [x]
  (cql/->CQLFn x "dateOf(%s)"))

(defn blob->type
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/->CQLFn x "blobAsType(%s)"))

(defn type->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/->CQLFn x "typeAsBlob(%s)"))

(defn blob->bigint
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/->CQLFn x "blobAsBigint(%s)"))

(defn bigint->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql/->CQLFn x "bigintAsBlob(%s)"))


;; Sugar for collection types

(defn coll-type
  "Helps with the generation of Collection types definitions.
Takes a CQL type as keyword and it's arguments: ex (coll-type :map :int :uuid).
The possible collection types are :map, :list and :set."
  [t & spec]
  (format "%s<%s>"
          (name t)
          (cql/join-comma (map name spec))))

(def
  ^{:doc "Generates a map type definition, takes 2 arguments, for
  key and value types"}
  map-type
  (partial coll-type :map))

(def
  ^{:doc "Generates a list type definition, takes a single argument
  indicating the list elements type"}
  list-type
  (partial coll-type :list))

(def
  ^{:doc "Generates a set type definition, takes a single argument
  indicating the set elements type"}
  set-type
  (partial coll-type :set))

;; Utilities

(defn apply-map
  "Takes a generated prepared query with its arg vector containing
  keywords for placeholders and maps the supplied map to it"
  [[query placeholders] parameter-map]
  [query (replace parameter-map placeholders)])
