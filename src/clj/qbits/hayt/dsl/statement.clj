(ns qbits.hayt.dsl.statement
  (:refer-clojure :exclude [update]))

(defn select
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#selectStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* where
* order-by
* limit
* only-if"
  [table & clauses]
  (into {:select table :columns :*} clauses))

(defn insert
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using
* if-exists"
  [table & clauses]
  (into {:insert table} clauses))

(defn update
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* only-if
* if-exists"
  [table & clauses]
  (into {:update table} clauses))

(defn delete
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* only-if"
  [table & clauses]
  (into {:delete table :columns :*} clauses))

(defn truncate
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#truncateStmt

Takes a table identifier."
  [table]
  {:truncate table})

(defn drop-keyspace
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#dropKeyspaceStmt

Takes a keyspace identifier and additional clauses:
* if-exists"
  [keyspace & clauses]
  (into {:drop-keyspace keyspace} clauses))

(defn drop-table
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#dropTableStmt

Takes a table identifier and additional clauses:
* if-exists"
  [table & clauses]
  (into {:drop-table table} clauses))

(defn drop-type
  "http://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlRefDropType.html

Takes a type identifier and additional clauses:
* if-exists"
  [type & clauses]
  (into {:drop-type type} clauses))


(defn drop-columnfamily
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#dropTableStmt

Takes a column family identifier and additional clauses:
* if-exists"
  [cf & clauses]
  (into {:drop-columnfamily cf} clauses))

(defn drop-trigger
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#dropTriggerStmt

Takes a trigger identifier and a table identifier"
  [trigger table]
  {:drop-trigger trigger :on table})

(defn drop-index
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#dropIndexStmt

Takes an index identifier and additional clauses:
* if-exists"
  [index & clauses]
  (into {:drop-index index} clauses))

(defn create-index
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#createIndexStmt

Takes a table identifier and additional clause arguments:

* index-column
* index-name
* custom
* on (overwrites table id)"
  [table name & clauses]
  (into {:create-index name :custom false :on table} clauses))

(defn create-keyspace
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with"
  [keyspace & clauses]
  (into {:create-keyspace keyspace} clauses))

(defn create-trigger
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#createTriggerStmt"
  [trigger table using]
  {:create-trigger trigger :on table :using using})

(defn create-type
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#createTypeStmt"
  [type & clauses]
  (into {:create-type type} clauses))

(defn create-table
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with"
  [table & clauses]
  (into {:create-table table} clauses))

(defn create-index
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#createIndexStmt

Takes a table identifier and additional clause arguments:

* index-column
* index-name
* custom
* on (overwrites table id)"
  [table name & clauses]
  (into {:create-index name :custom false :on table} clauses))

(defn alter-type
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#alterTypeStmt"
  [type & clauses]
  (into {:alter-type type} clauses))

(defn alter-table
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add-column
* alter-column
* rename-column
* drop-column
* with"
  [table & clauses]
  (into {:alter-table table} clauses))

(defn alter-columnfamily
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#alterTableStmt

Takes a columnfamiliy identifier and additional clause arguments:

* alter-column
* add-column
* alter-column
* rename-column
* drop-column
* with"
  [columnfamily & clauses]
  (into {:alter-columnfamily columnfamily} clauses))

(def ^{:deprecated "1.5.0"} alter-column-family alter-columnfamily)

(defn alter-keyspace
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause."
  [keyspace & clauses]
  (into {:alter-keyspace keyspace} clauses))

(defn batch
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#batchStmt

Takes hayt queries  optional clauses:
* queries
* using
* counter
* logged "
  [& clauses]
  (into {:logged true} clauses))

(defn use-keyspace
  "http://cassandra.apache.org/doc/old/CQL-3.0.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  {:use-keyspace keyspace})

(defn grant
  "Takes clauses:
* resource
* user"
  [perm & clauses]
  (into {:grant perm} clauses))

(defn revoke
  "Takes clauses:
* resource
* user"
  [perm & clauses]
  (into {:revoke perm} clauses))

(defn create-user
  "Takes clauses:
* password
* superuser (defaults to false)"
  [user & clauses]
  (into {:create-user user :superuser false} clauses))

(defn alter-user
  "Takes clauses:
* password
* superuser (defaults to false)"
  [user & clauses]
  (into {:alter-user user :superuser false} clauses))

(defn drop-user
  "Takes a user identifier
* if-exists"
  [user & clauses]
  (into {:drop-user user} clauses))

(defn list-users
  ""
  []
  {:list-users nil})

(defn list-perm
  "Takes clauses:
* perm (defaults to ALL if not supplied)
* user
* resource
* recursive (defaults to true)"
  [& clauses]
  (into {:list-perm :ALL :recursive true} clauses))
