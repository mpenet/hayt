(ns qbits.hayt.cql
  "CQL3 ref: http://cassandra.apache.org/doc/cql3/CQL.html or
https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions

This one is really up to date:
https://github.com/apache/cassandra/blob/cassandra-1.2/src/java/org/apache/cassandra/cql3/Cql.g"
  (:require [clojure.string :as string]))

(declare emit-query emit-row)
(def ^:dynamic *param-stack*)
(def ^:dynamic *prepared-statement* false)

;; Wraps a CQL function (a template to clj.core/format and its
;; argument for later encoding.
(defrecord CQLFn [name args])
(defrecord CQLSafe [value])

(defn cql-safe
  [x]
  (->CQLSafe x))

(defn cql-fn
  [name & args]
  (map->CQLFn {:name name :args args}))

(defn maybe-parameterize!
  ([x f]
     (if *prepared-statement*
       (do (swap! *param-stack* conj x) "?")
       (f x)))
  ([x]
     (maybe-parameterize! x identity)))

(def join-and #(string/join " AND " %))
(def join-spaced #(string/join " " %))
(def join-comma #(string/join ", " %))
(def join-lf #(string/join "\n" %))
(def format-eq #(str %1 " = " %2))
(def format-kv #(str %1 " : "  %2))
(def quote-string #(str "'" (string/replace % "'" "''") "'"))
(def dquote-string #(str "\"" (string/replace % "\" " "\"\"") "\""))
(def wrap-parens #(str "(" % ")"))
(def wrap-brackets #(str "{" % "}"))
(def wrap-sqbrackets #(str "[" % "]"))
(def kw->c*const #(-> % name string/upper-case (.replaceAll "-" "_")))
(def terminate #(str % ";"))

(defprotocol CQLEntities
  (cql-identifier [x]
    "Encodes CQL identifiers")
  (cql-value [x]
    "Encodes a CQL value, pushing it to *param-stack* if
     it's a prepared statement and replacing it with ?"))

(extend-protocol CQLEntities

  String
  (cql-identifier [x] (dquote-string x))
  (cql-value [x]
    (maybe-parameterize! x #(quote-string %)))

  clojure.lang.Keyword
  (cql-identifier [x] (name x))
  (cql-value [x]
    (maybe-parameterize! x #(cql-value (name %))))

  ;; Collections are just for cassandra collection types, not to
  ;; generate query parts
  clojure.lang.IPersistentSet
  (cql-value [x]
    (maybe-parameterize! x
      #(->> (map cql-value %)
            join-comma
            wrap-brackets)))

  clojure.lang.IPersistentMap
  (cql-identifier [x]
    (let [[coll k] (first x) ]
      ;; handles foo['bar'] lookups
      (str (cql-identifier coll)
           (wrap-sqbrackets (cql-value k)))))
  (cql-value [x]
    (maybe-parameterize! x
     #(->> %
          (map (fn [[k v]]
                 (format-kv (cql-value k)
                            (cql-value v))))
          join-comma
          wrap-brackets)))

  clojure.lang.Sequential
  (cql-value [x]
    (maybe-parameterize! x
     #(->> (map cql-value %)
          join-comma
          wrap-sqbrackets)))

  ;; CQL Function are always safe, their arguments might not be though
  CQLFn
  (cql-identifier [{fn-name :name  args :args}]
    (str (name fn-name)
         (wrap-parens (join-comma (map cql-identifier args)))))
  (cql-value [{fn-name :name  args :args}]
    (str (name fn-name)
         (wrap-parens (join-comma (map cql-value args)))))

  CQLSafe
  (cql-identifier [x] x)
  (cql-value [x] x)

  nil
  (cql-value [x] (maybe-parameterize! x))

  Object
  (cql-identifier [x] x)
  (cql-value [x] (maybe-parameterize! x)))

(def operators {= "="
                > ">"
                < "<"
                <= "<="
                >= ">="
                + "+"
                - "-"})
(defn operator?
  [op]
  (or (keyword? op)
      (get operators op)))

(defn option-value
  [x]
  (if (or (number? x)
          (instance? Boolean x))
    x
    (quote-string (name x))))

(defn option-map [m]
  (->> m
       (map (fn [[k v]]
              (format-kv (quote-string (name k))
                         (option-value v))))
       join-comma
       wrap-brackets))

(defn where-sequential-entry [column [op value]]
  (let [col-name (cql-identifier column)]
    (cond
      (= :in op)
      (str col-name
           " IN "
           (->> (map cql-value value)
                join-comma
                wrap-parens))

      (fn? op)
      (str col-name
           " " (operators op) " "
           (cql-value value))

      (keyword? op)
      (str col-name
           " " (name op) " "
           (cql-value value)))))

;; x and y can be an operator or a value
(defn counter [column [x y]]
  (let [identifier (cql-identifier column)]
    (->> (if (operator? x)
           [identifier (operators x) (cql-value y)]
           [(cql-value x) (operators y) identifier])
         join-spaced
         (format-eq identifier))))

(def emit
  {;; entry clauses
   :select
   (fn [q table]
     (str "SELECT "
          ((emit :columns) q (:columns q))
          " "
          (emit-row (assoc q :from table)
                    [:from :where :order-by :limit :allow-filtering])))

   :insert
   (fn [q table]
     (str "INSERT INTO "
          (cql-identifier table)
          " "
          (emit-row q [:values :using])))

   :update
   (fn [q table]
     (str "UPDATE "
          (cql-identifier table)
          " "
          (emit-row q [:using :set-columns :where])))

   :delete
   (fn [q table]
     (str "DELETE "
          ((emit :columns) q (:columns q))
          " "
          (emit-row (assoc q :from table)
                    [:from :using :where])))

   :drop-index
   (fn [q index]
     (str "DROP INDEX " (cql-identifier index)))

   :drop-table
   (fn [q table]
     (str "DROP TABLE " (cql-identifier table)))

   :drop-keyspace
   (fn [q keyspace]
     (str "DROP KEYSPACE " (cql-identifier keyspace)))

   :use-keyspace
   (fn [q ks]
     (str "USE " (cql-identifier ks)))

   :truncate
   (fn [q ks]
     (str "TRUNCATE " (cql-identifier ks)))

   :grant
   (fn [q permission]
     (str "GRANT "
          ((emit :perm) q permission)
          " "
          (emit-row q [:resource :user])))

   :revoke
   (fn [q permission]
     (str "REVOKE "
          ((emit :perm) q permission)
          " "
          (emit-row q [:resource :user])))

   :create-index
   (fn [{:keys [custom with]
         :as q}
        column]
     (str "CREATE "
          (when custom "CUSTOM ")
          "INDEX "
          (emit-row q [:index-name :on])
          " " (wrap-parens (cql-identifier column))
          (when (and custom with)
            (str " " ((emit :with) q with)))))

   :create-user
   (fn [q user]
      (str "CREATE USER "
           (cql-identifier user)
           " "
           (emit-row q [:password :superuser])))

   :alter-user
   (fn [q user]
      (str "ALTER USER "
           (cql-identifier user)
           " "
           (emit-row q [:password :superuser])))

   :drop-user
   (fn [q user]
     (str "DROP USER " (cql-identifier user)))

   :list-users
   (constantly "LIST USERS")

   :perm
   (fn [q perm]
     (let [raw-perm (kw->c*const perm)]
       (str "PERMISSION" (when (= "ALL" raw-perm) "S") " " raw-perm)))

   :list-perm
   (fn [q perm]
     (str "LIST "
          ((emit :perm) q perm)
          " "
          (emit-row q [:resource :user :recursive])))

   :create-table
   (fn [q table]
     (str "CREATE TABLE " (cql-identifier table)
          " "
          (emit-row q [:column-definitions :with])))

   :alter-table
   (fn [q table]
     (str "ALTER TABLE " (cql-identifier table)
          " "
          (emit-row q [:alter-column :add-column :rename-column :with])))

   :alter-column-family
   (fn [q cf]
     (str "ALTER COLUMNFAMILY " (cql-identifier cf)
          " "
          (emit-row q [:alter-column :add-column :rename-column :with])))

   :alter-keyspace
   (fn [q ks]
     (str "ALTER KEYSPACE " (cql-identifier ks)
          " "
          (emit-row q [:with])))

   :create-keyspace
   (fn [q ks]
     (str "CREATE KEYSPACE " (cql-identifier ks)
          " "
          (emit-row q [:with])))

   :resource
   (fn [q resource]
     ((emit :on) q resource))

   :user
   (fn [q user]
     (cond
      (contains? q :list-perm)
      ((emit :of) q user)

      (contains? q :revoke)
      ((emit :from) q user)

      (contains? q :grant)
      ((emit :to) q user)))

   :on
   (fn [q on]
     (str "ON " (cql-identifier on)))

   :to
   (fn [q to]
     (str "TO " (cql-identifier to)))

   :of
   (fn [q on]
     (str "OF " (cql-identifier on)))

   :from
   (fn [q table]
     (str "FROM " (cql-identifier table)))

   :into
   (fn [q table]
     (str "INTO " (cql-identifier table)))


   :columns
   (fn [q columns]
     (if (sequential? columns)
       (join-comma (map cql-identifier columns))
       (cql-identifier columns)))

   :where
   (fn [q clauses]
     (->> clauses
          (map (fn [[k v]]
                 (if (sequential? v)
                   ;; Sequence, we do the complex thing first
                   (where-sequential-entry k v)
                   ;; else we just append if its a simple map val
                   (format-eq (cql-identifier k) (cql-value v)))))
          join-and
          (str "WHERE ")))

   :order-by
   (fn [q columns]
     (->> columns
          (map (fn [col-values] ;; Values are a pair of col and order
                 (join-spaced (map cql-identifier col-values))))
          join-comma
          (str "ORDER BY ")))

   :primary-key
   (fn [q primary-key]
     (->> (if (sequential? primary-key)
            (map (fn [pk]
                   (if (sequential? pk)
                     (->  (map cql-identifier pk)
                          join-comma
                          wrap-parens)
                     (cql-identifier pk)))
                 primary-key)
            (cql-identifier primary-key))
          join-comma
          wrap-parens
          (str "PRIMARY KEY ")))

   :column-definitions
   (fn [q {:keys [primary-key] :as column-definitions}]
     (-> (mapv (fn [[k v]]
                 (join-spaced [(cql-identifier k)
                               (cql-identifier v)]))
               (dissoc column-definitions :primary-key))
         (conj ((:primary-key emit) q primary-key))
         join-comma
         wrap-parens))

   :limit
   (fn [q limit]
     (str "LIMIT " limit))

   :values
   (fn [q values-map]
     (let [columns (keys values-map)
           values (vals values-map)]
       (str (wrap-parens (join-comma (map cql-identifier columns)))
            " VALUES "
            (wrap-parens (join-comma (map cql-value values))))))

   :set-columns
   (fn [q values]
     (->> values
          (map (fn [[k v]]
                 (if (vector? v)
                   (counter k v)
                   (format-eq (cql-identifier k)
                              (cql-value v)))))
          join-comma
          (str "SET ")))

   :using
   (fn [q args]
     (->> args
          (map (fn [[n value]]
                 (str (-> n name string/upper-case)
                      " " (cql-identifier value))))
          join-and
          (str "USING ")))

   :compact-storage
   (fn [q v]
     (when v "COMPACT STORAGE"))

   :allow-filtering
   (fn [q v]
     (when v "ALLOW FILTERING"))

   :alter-column
   (fn [q [identifier type]]
     (format "ALTER %s TYPE %s"
             (cql-identifier identifier)
             (cql-identifier type)))


   :rename-column
   (fn [q [old-name new-name]]
     (format "RENAME %s TO %s"
             (cql-identifier old-name)
             (cql-identifier new-name)))

   :add-column
   (fn [q [identifier type]]
     (format "ADD %s %s"
             (cql-identifier identifier)
             (cql-identifier type)))

   :clustering-order
   (fn [q columns]
     (->> columns
          (map (fn [col-values] ;; Values are a pair of col and order
                 (join-spaced (map cql-identifier col-values))))
          join-comma
          wrap-parens
          (str "CLUSTERING ORDER BY ")))

   :with
   (fn [q value-map]
     (->> (for [[k v] value-map]
            (if-let [with-entry (k emit)]
              (with-entry q v)
              (format-eq (cql-identifier k)
                         (if (map? v)
                           (option-map v)
                           (option-value v)))))
          join-and
          (str "WITH ")))

   :password
   (fn [q pwd]
     ;; not sure if its a cql-id or cql-val
     (str "WITH PASSWORD " (cql-identifier pwd)))

   :superuser
   (fn [q superuser?]
     (if superuser? "SUPERUSER" "NOSUPERUSER"))

   :recursive
   (fn [q recursive]
     (when-not recursive "NORECURSIVE"))

   :index-column
   (fn [q index-column]
     (wrap-parens (cql-identifier index-column)))

   :batch
   (fn [{:keys [logged counter]
         :as q} queries]
     (str "BEGIN"
          (when-not logged " UNLOGGED")
          (when counter " COUNTER")
          " BATCH "
          (when-let [using (:using q)]
            (str ((emit :using) q using) " "))
          (->> (let [subqs (join-lf (map emit-query queries))]
                 (if *prepared-statement*
                   [subqs @*param-stack*])
                 subqs)
               (format "\n%s\n"))
          " APPLY BATCH"))

   :queries
   (fn [q queries]
     (->> (let [subqs (join-lf (map emit-query queries))]
            (if *prepared-statement*
              [subqs @*param-stack*])
            subqs)
          (format "\n%s\n")))})

(def emit-catch-all (fn [q x] (cql-identifier x)))

(def entry-clauses #{:select :insert :update :delete :use-keyspace :truncate
                     :drop-index :drop-table :drop-keyspace :create-index :grant
                     :revoke :create-user :alter-user :drop-user :list-users
                     :list-perm :batch :create-table :alter-table
                     :alter-column-family :alter-keyspace :create-keyspace})

(defn find-entry-clause
  "Finds entry point key from query map"
  [qmap]
  (some entry-clauses (keys qmap)))

(defn emit-row
  [row template]
  (->> template
       (map (fn [token]
              (let [context (get row token ::empty)]
                (when-not (= ::empty context)
                  ((get emit token emit-catch-all) row context)))))
       (remove nil?)
       (join-spaced)))

(defn emit-query [query]
  (let [entry-point (find-entry-clause query)]
    (terminate ((emit entry-point) query (entry-point query)))))

(defn ->raw
  "Compiles a hayt query into its raw/string value"
  [query]
  (binding [*prepared-statement* false]
    (emit-query query)))

(defn ->prepared
  "Compiles a hayt query into a vector composed of the prepared string
  query and a vector of parameters."
  [query]
  (binding [*prepared-statement* true
            *param-stack* (atom [])]
    [(emit-query query)
     @*param-stack*]))
