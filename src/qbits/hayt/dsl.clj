(ns qbits.hayt.dsl)

(defn select
  "http://cassandra.apache.org/doc/cql3/CQL.html#selectStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* where
* order-by
* limit
* only-if"
  [table & clauses]
  (into {:select table :columns :*} clauses))

(defn insert
  "http://cassandra.apache.org/doc/cql3/CQL.html#insertStmt

Takes a table identifier and additional clause arguments:
* values
* using"
  [table & clauses]
  (into {:insert table} clauses))

(defn update
  "http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt

Takes a table identifier and additional clause arguments:

* using
* set-columns
* where
* only-if
* if-not-exists"
  [table & clauses]
  (into {:update table} clauses))

(defn delete
  "http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt

Takes a table identifier and additional clause arguments:

* columns (defaults to *)
* using
* where
* only-if"
  [table & clauses]
  (into {:delete table :columns :*} clauses))

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
* custom
* on (overwrites table id)"
  [table name & clauses]
  (into {:create-index name :custom false :on table} clauses))

(defn create-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt

Takes a keyspace identifier and clauses:
* with"
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
* add-column
* alter-column
* rename-column
* drop-column
* with"
  [table & clauses]
  (into {:alter-table table} clauses))

(defn alter-column-family
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterTableStmt

Takes a column-familiy identifier and additional clause arguments:

* alter-column
* add-column
* alter-column
* rename-column
* drop-column
* with"
  [column-family & clauses]
  (into {:alter-column-family column-family} clauses))

(defn alter-keyspace
  "http://cassandra.apache.org/doc/cql3/CQL.html#alterKeyspaceStmt

Takes a keyspace identifier and a `with` clause."
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
  [user]
  {:drop-user user})

(defn list-users
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

;; Clauses

(defn columns
  "Clause: takes columns identifiers"
  [& columns]
  {:columns columns})

(defn column-definitions
  "Clause: Takes a map of columns definitions (keys are identifiers ,
   values, types), to be used with create-table."
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  "Clause: Sets USING, takes keyword/value pairs for :timestamp and :ttl"
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

(defn only-if
  "Clause: takes a map or a vector of pairs to compose the if
clause of a update/delete query"
  [args]
  {:if args})

(defn if-not-exists
  "Clause: Apply only if the row does not exist"
  ([b]
     {:if-not-exists b})
  ([]
     (if-not-exists true)))

(defn values
  "Clause: Takes a map of columns to be inserted"
  [values]
  {:values values})

(defn set-columns
  "Clause: Takes a map of columns to be updated"
  [values]
  {:set-columns values})

(defn with
  "Clause: compiles to a CQL with clause (possibly nested maps)"
  [values]
  {:with values})

(defn index-name
  "Clause: Takes an index identifier"
  [value]
  {:index-name value})

(defn alter-column
  "Clause: takes a table identifier and a column type"
  [identifier type]
  {:alter-column [identifier type]})

(defn add-column
  "Clause: takes a table identifier and a column type"
  [identifier type]
  {:add-column [identifier type]})

(defn rename-column
  "Clause: rename from old-name to new-name"
  [old-name new-name]
  {:rename-column [old-name new-name]})

(defn drop-column
  "Clause: Takes a column Identifier"
  [id]
  {:drop-column id})

(defn allow-filtering
  "Clause: sets ALLOW FILTERING on select queries, defaults to true is
   used without a value"
  ([value]
     {:allow-filtering value})
  ([]
     (allow-filtering true)))

(defn logged
  "Clause: Sets LOGGED/UNLOGGED attribute on BATCH queries"
  ([value]
     {:logged value})
  ([]
     (logged true)))

(defn counter
  "Clause: Sets COUNTER attribute on BATCH queries"
  ([value]
     {:counter value})
  ([]
     (counter true)))

(defn superuser
  "Clause: To be used with alter-user and create-user, sets superuser status"
  ([value]
     {:superuser value})
  ([]
     (superuser true)))

(defn password
  "Clause: To be used with alter-user and create-user, sets password"
  [value]
  {:password value})

(defn recursive
  "Clause: Sets recusivity on list-perm (LIST PERMISSION) queries"
  ([value]
     {:recursive value})
  ([]
     (recursive true)))

(defn resource
  "Clause: Sets resource to be modified/used with grant or list-perm"
  [value]
  {:resource value})

(defn user
  "Clause: Sets user to be modified/used with grant or list-perm"
  [value]
  {:user value})

(defn perm
  "Clause: Sets permission to be listed with list-perm"
  [value]
  {:list-perm value})

(defn custom
  "Clause: Sets CUSTOM status on create-index query"
  ([x]
     {:custom x})
  ([]
     (custom true)))
