(ns qbits.hayt
  (:require [qbits.hayt.cql :as cql])
  (:import [java.util Date]))

(defprotocol PCompile
  (->raw [x])
  (->prepared [x]))

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
  [& columns]
  {:select columns})

(defn from
  [x]
  {:from x})

(defn insert
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using
* table (optionaly using composition)"
  [table]
  {:insert table})

(defn update
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* table (optionaly using composition)"
  [table]
  {:update table})

(defn delete
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* table (optionaly using composition)"
  [& columns]
  {:delete columns})

(defn truncate
  "http://cassandra.apache.org/doc/cql3/CQL.html#truncateStmt

Takes a table identifier."
  [table]
  {:truncate table})

(defn drop-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropKeyspaceStmt

Takes a keyspace identifier"
  [keyspace]
  {:drop-keyspace keyspace})

(defn drop-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropTableStmt

Takes a table identifier"
  [table]
  {:drop-table table})

(defn drop-index
  "http://cassandra.apache.org/doc/cql3/CQL.html#dropIndexStmt

Takes an index identifier."
  [index]
  {:drop-index index})

(defn create-index
  "http://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt

Takes a table identifier and additional clause arguments:

* index-column
* index-name
* table (optionaly using composition)"
  [index-column]
  {:create-index index-column})

(defn create-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  {:create-keyspace keyspace})

(defn create-table
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with
* table (optionaly using composition)"
  [table]
  {:create-table table})

(defn alter-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename
* table (optionaly using composition)"
  [table]
  {:alter-table table})

(defn alter-column-family
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a column-familiy identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename
* column-family (optionaly using composition)"
  [column-family]
  {:alter-column-family column-family})

(defn alter-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause.
* keyspace (optionaly using composition)"
  [keyspace]
  {:alter-keyspace keyspace})

(defn batch
  "http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt

Takes hayt queries  optional clauses:
* using
* counter
* logged "
  [& queries]
  {:batch queries :logged true})

(defn use-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  {:use-keyspace keyspace})

(defn grant
  "Takes clauses:
* permission
* resource
* user (optionaly using composition)"
  [permission]
  {:grant permission})

(defn revoke
  "Takes clauses:
* permission
* resource
* user (optionaly using composition)"
  [permission]
  {:revoke permission})

(defn create-user
  "Takes clauses:
* password
* superuser (defaults to false)
* user (optionaly using composition)"
  [user]
  {:create-user user
   :superuser false})

(defn alter-user
  "Takes clauses:
* password
* superuser (defaults to false)
* user (optionaly using composition)"
  [user]
  {:alter-user user :superuser false})

(defn drop-user
  [user]
  {:drop-user user})

(defn list-users
  []
  {:list-users nil})

(defn list-permission
  "Takes clauses:
* permissions (defaults to ALL if not supplied)
* user
* resource
* recursive (defaults to true)"
  [perm]
  {:list-permission perm :recursive true})

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
  {:using (apply hash-map args)})

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

(defn on
  "Clause: "
  [x]
  {:on x})

(defn to
  "Clause: "
  [x]
  {:to x})

(defn of
  "Clause: "
  [x]
  {:of x})

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

(defn superuser
  "Clause: "
  [value]
  {:superuser value})

(defn with-password
  "Clause: "
  [value]
  {:with-password value})

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
  (into q clauses))

(defn cql-safe
  [x]
  (cql/CQLSafe. x))

;; CQL3 functions

(defn cql-fn
  [name & args]
  (cql/map->CQLFn {:name name :args args}))

(def ^{:doc "Returns a now() CQL function"} now
  (constantly (cql-fn "now")))

(def ^{:doc "Returns a count(*) CQL function"} count*
  (constantly (cql-fn "COUNT" :*)))

(def ^{:doc "Returns a count(1) CQL function"} count1
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
  [token]
  (cql-fn "token" token))

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

(defn blob->type
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql-fn "blobAsType" x))

(defn type->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql-fn "typeAsBlob" x))

(defn blob->bigint
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql-fn "blobAsBigint" x))

(defn bigint->blob
  "https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
Only available in 3.0.2"
  [x]
  (cql-fn "bigintAsBlob" x))


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
