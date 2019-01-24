(ns qbits.hayt.dsl.clause
  (:refer-clojure :exclude [group-by]))

(defn columns
  "Clause: takes columns identifiers
ex: (columns :foo \"bar\" :baz) "
  [& columns]
  {:columns columns})

(defn column-definitions
  "Clause: Takes a map of columns definitions (keys are identifiers ,
   values, types), to be used with create-table."
  [column-definitions]
  {:column-definitions column-definitions})

(defn using
  "Clause: Sets USING, takes keyword/value pairs for :timestamp and :ttl"
  ([opts] {:using opts})
  ([x y & more]
     (using (partition 2 (concat [x y] more)))))

(defn limit
  "Clause: Sets LIMIT, takes a numeric value"
  [n]
  {:limit n})

(defn group-by
  "Clause: expects 1 or more columns.
  ex: (group-by col1 col2)"
  [& columns] {:group-by columns})

(defn order-by
  "Clause: takes vectors of 2 elements, where the first is the column
  identifier and the second is the ordering as keyword.
  ex: (order-by [:asc :desc])"
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

(defn where'
  "Same as `where` but for people who prefer unrolled args"
  [& args]
  {:where args})

(defn where1
  "backward compatible with hayt 1.0 and 2.0 betas"
  ([args]
     {:where (map (fn [[k v]]
                    (if (sequential? v)
                      [(first v) k (second v)]
                      [k v]))
                  args)})
  ([x y & more]
     (where1 (partition 2 (concat [x y] more)))))

(defn only-if
  "Clause: takes a map or a vector of pairs (same as `where`) to compose the if
clause of a update/delete query"
  [args]
  {:if args})

(defn only-if'
  "Clause: takes a map or a vector of pairs (same as `where`) to compose the if
clause of a update/delete query"
  [& args]
  {:if args})

(defn if-exists
  "Clause: Apply only if the target exists"
  ([b]
     {:if-exists b})
  ([]
     (if-exists true)))

(defn if-not-exists
  "DEPRECATED use (if-exists false)
Clause: Apply only if the row does not exist"
  ^{:deprecated "1.2.0"}
  ([b]
     (if-exists (not b)))
  ([]
     (if-exists false)))

(defn values
  "Clause: Takes a map of columns to be inserted"
  ([values]
     {:values values})
  ([x y & more]
     (values (partition 2 (concat [x y] more)))))

(defn set-columns
  "Clause: Takes a map of columns to be updated"
  ([values]
     {:set-columns values})
  ([x y & more]
     (set-columns (partition 2 (concat [x y] more)))))

(defn with
  "Clause: compiles to a CQL with clause (possibly nested maps)"
  ([values]
     {:with values})
  ([x y & more]
     (with (partition 2 (concat [x y] more)))))

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
