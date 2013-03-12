(ns qbits.hayt.dsl)

(defn mv-key [pm from to]
    (assoc (dissoc pm from) to (get pm from)))

(defn select
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* where
* order-by
* limit
* table (optionaly using composition)"
  [table & clauses]
  (mv-key (into {:from table :columns :*} clauses)
          :columns :select))

(defn from
  [x]
  {:from x})

(defn insert
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using
* table (optionaly using composition)"
  [table & clauses]
  (into {:insert table} clauses))

(defn update
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* table (optionaly using composition)"
  [table & clauses]
  (into {:update table} clauses))

(defn delete
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* table (optionaly using composition)"
  [table & clauses]
  (mv-key (into {:from table :columns :*} clauses)
          :columns :delete))

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
  [name & clauses]
  (into {:create-index name} clauses))

(defn create-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (into {:create-keyspace keyspace} clauses))

(defn create-table
  "Takes a table identifier and additional clause arguments:

* column-definitions
* with"
  [table & clauses]
  (into {:create-table table} clauses))

(defn alter-table
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a table identifier and additional clause arguments:

* alter-column
* add
* with
* alter
* rename"
  [table & clauses]
  (into {:alter-table table} clauses))

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
  (into {:alter-column-family column-family} clauses))

(defn alter-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause.
* keyspace (optionaly using composition)"
  [keyspace & clauses]
  (into {:alter-keyspace keyspace} clauses))

(defn batch
  "http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt

Takes hayt queries  optional clauses:
* using
* counter
* logged "
  [& clauses]
  (into {:logged true} clauses))

(defn use-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#useStmt

Takes a keyspace identifier"
  [keyspace]
  {:use-keyspace keyspace})

(defn grant-permission
  "Takes clauses:"
  [permission & clauses]
  (into {:grant permission} clauses))

(defn revoke-permission
  "Takes clauses:
* permission
* resource
* user (optionaly using composition)"
  [permission & clauses]
  (into {:revoke permission} clauses))

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
  [user]
  {:drop-user user})

(defn list-users
  []
  {:list-users nil})

(defn list-permissions
  "Takes clauses:
* permission (defaults to ALL if not supplied)
* user
* resource
* recursive (defaults to true)"
  [perm & clauses]
  (into {:list-permissions perm :recursive true} clauses))

;; Clauses

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
  {:batch queries})

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

(defn recursive
  "Clause: "
  [value]
  {:recursive value})
