(ns qbits.hayt.cql
  "CQL3 ref: http://cassandra.apache.org/doc/cql3/CQL.html or
https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions

This one is really up to date:
https://github.com/apache/cassandra/blob/cassandra-1.2/src/java/org/apache/cassandra/cql3/Cql.g
And a useful test suite: https://github.com/riptano/cassandra-dtest/blob/master/cql_tests.py"
  (:import
   (org.apache.commons.lang3 StringUtils)
   (java.nio ByteBuffer)
   (java.util Date)
   (java.net InetAddress)
   (qbits.hayt Hex)))

(declare emit-query emit-row)

(defprotocol CQLEntities
  (cql-identifier [x]
    "Encodes CQL identifiers")
  (cql-value [x]
    "Encodes a CQL value"))

;; Wraps a CQL function (a template to clj.core/format and its
;; argument for later encoding.
(defrecord CQLFn [name args])
(defrecord CQLRaw [value])
(defrecord CQLRawPreparable [value])
(defrecord CQLNamespaced [value])
(defrecord CQLComposite [value])
(defrecord CQLUserType [value])

(defmacro str* [& xs]
  (let [size (count xs)]
    `(->
      ~(if (= size 1)
         (first xs)
         `(doto (StringBuilder.)
            ~@(for [x xs]
                (list '.append x))))
      .toString)))

(defmacro str+ [sb & xs]
  (let [size (count xs)
        sb (vary-meta sb assoc :tag 'java.lang.StringBuilder)]
    `(doto ~sb
       ~@(for [x xs]
           (list '.append x)))))

(defn join [^java.lang.Iterable coll ^String sep]
  (when (seq coll)
    (StringUtils/join coll sep)))

(def join-and #(join % " AND "))
(def join-spaced #(join % " "))
(def join-comma #(join % ", "))
(def interpose-comma (interpose ", "))
(def join-dot #(join % "."))
(def join-lf #(join % "\n" ))
(def format-eq #(str* %1 " = " %2))
(def format-kv #(str* %1 " : "  %2))
(def quote-string #(str* "'" (StringUtils/replace % "'" "''") "'"))
(def dquote-string #(str* "\"" (StringUtils/replace % "\" " "\"\"") "\""))
(def wrap-parens #(str* "(" (or % "") ")"))
(def wrap-brackets #(str* "{" % "}"))
(def wrap-sqbrackets #(str* "[" % "]"))
(def kw->c*const #(-> (name %)
                      StringUtils/upperCase
                      (StringUtils/replaceChars \- \_)))
(def terminate #(str+ % \; ))
(def sequential-or-set? (some-fn sequential? set?))

(def cql-values-join-comma-xform
  (comp (map #'cql-value)
        interpose-comma))

(def cql-identifiers-join-comma-xform
  (comp (map #'cql-identifier)
        interpose-comma))


(defn str-fold
  ([] (StringBuilder.))
  ([^StringBuilder sb x]
   (.append sb x))
  ([^StringBuilder sb] (.toString sb)))

(defn cql-values-join-comma [xs]
  (transduce cql-values-join-comma-xform
             str-fold
             xs))

(defn cql-identifiers-join-comma
  [xs]
  (transduce cql-identifiers-join-comma-xform
             str-fold
             xs))

(extend-protocol CQLEntities

  (Class/forName "[B")
  (cql-identifier [x]
    (cql-value (ByteBuffer/wrap x)))
  (cql-value [x]
    (cql-value (ByteBuffer/wrap x)))

  ByteBuffer
  (cql-identifier [x]
    (Hex/toHexString x))
  (cql-value [x]
    (Hex/toHexString x))

  String
  (cql-identifier [x] (dquote-string x))
  (cql-value [x] (quote-string x))

  clojure.lang.Keyword
  (cql-identifier [x] (name x))
  (cql-value [x] (str* x))

  Date
  (cql-value [x]
    (.getTime ^Date x))

  InetAddress
  (cql-value [x]
    (.getHostAddress ^InetAddress x))

  ;; Collections are just for cassandra collection types, not to
  ;; generate query parts
  clojure.lang.IPersistentSet
  (cql-value [x]
    (->> x
         cql-values-join-comma
         wrap-brackets))

  clojure.lang.IPersistentMap
  (cql-identifier [x]
    (let [[coll k] (first x) ]
      ;; handles foo['bar'] lookups
      (str* (cql-identifier coll)
           (wrap-sqbrackets (cql-value k)))))

  (cql-value [x]
    (->> x
         (map (fn [[k v]]
                (format-kv (cql-value k)
                           (cql-value v))))
         join-comma
         wrap-brackets))

  clojure.lang.Sequential
  (cql-value [x]
    (->> x
         cql-values-join-comma
         wrap-sqbrackets))

  CQLUserType
  (cql-identifier [x]
    (->> (:value x)
         (map (fn [[k v]]
                (format-kv (cql-identifier k)
                           (cql-value v))))
         join-comma
         wrap-brackets))
  (cql-value [x]
    (->> (:value x)
         (map (fn [[k v]]
                (format-kv (cql-identifier k)
                           (cql-value v))))
         join-comma
         wrap-brackets))


  CQLComposite
  (cql-identifier [c]
    (->> (:value c)
         cql-identifiers-join-comma
         wrap-parens))
  (cql-value [c]
    (->> (:value c)
         cql-values-join-comma
         wrap-parens))


  ;; CQL Function are always safe, their arguments might not be though
  CQLFn
  (cql-identifier [{fn-name :name  args :args}]
    (str (name fn-name)
         (-> args
             cql-identifiers-join-comma
             wrap-parens)))
  (cql-value [{fn-name :name  args :args}]
    (str (name fn-name)
         (-> args
             cql-values-join-comma
             wrap-parens)))

  CQLRaw
  (cql-identifier [x] (:value x))
  (cql-value [x] (:value x))

  CQLRawPreparable
  (cql-identifier [x] (:value x))
  (cql-value [x] (:value x))

  clojure.lang.Symbol
  (cql-identifier [x] (str x))
  (cql-value [x] (str x))

  CQLNamespaced
  (cql-identifier [xs]
    (join-dot (map cql-identifier (:value xs))))

  nil
  (cql-value [x] "null")

  Object
  (cql-identifier [x] x)
  (cql-value [x] x))

(def contains-key (fn []))
(def contains (fn []))

(defonce operators
  (let [ops {'= "="
             '> ">"
             '< "<"
             '<= "<="
             '>= ">="
             '+ "+"
             '- "-"
             'contains "CONTAINS"
             'contains-key "CONTAINS KEY"}]
    (reduce-kv
     (fn [m k v]
       (-> m
           (assoc (keyword k) v)
           (assoc (eval k) v)))
     {}
     ops)))

(defn operator?
  [op]
  (get operators op))

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


;; secondary index clauses helpers
(defn query-cond-sequential-entry [op column value]
  (let [[column value] (if (sequential? column)
                         [(CQLComposite. column)
                          (CQLComposite. value)]
                         [column value] )
        col-name (cql-identifier column)]
    (if (identical? :in op)
      (str* col-name
            " IN "
            (if (sequential-or-set? value)
              (-> value
                  cql-values-join-comma
                  wrap-parens)
              (cql-value value)))
      (str* col-name
            " " (operators op) " "
            (cql-value value)))))

(defn query-cond
  [clauses]
  (->> clauses
       (map (fn [xs]
              (if (= 3 (count xs))
                (query-cond-sequential-entry (first xs)
                                             (second xs)
                                             (last xs))
                (query-cond-sequential-entry = (first xs) (second xs)))))
       join-and))

;; x and y can be an operator or a value
(defn counter [column [x y]]
  (let [identifier (cql-identifier column)]
    (->> (if (operator? x)
           [identifier (operators x) (cql-value y)]
           [(cql-value x) (operators y) identifier])
         join-spaced
         (format-eq identifier))))

(def emit
  { ;; entry clauses
   :select
   (fn [^StringBuilder sb q table]
     (-> sb
         (str+ "SELECT ")
         (emit-row (assoc q :from table)
                   [:columns :from :where :order-by :limit :allow-filtering])))
   :insert
   (fn [sb q table]
     (-> sb
         (str+ "INSERT INTO " (cql-identifier table) " ")
         (emit-row q [:values :if-exists :using])))

   :update
   (fn [sb q table]
     (-> sb
         (str+ "UPDATE " (cql-identifier table) " ")
         (emit-row q [:using :set-columns :where :if :if-exists])))

   :delete
   (fn [sb {:keys [columns] :as q} table]
     (let [q (assoc (if (identical? :* columns)
                      (dissoc q :columns)
                      q)
                    :from table)]
       (-> sb
           (str+ "DELETE ")
           (emit-row q [:columns :from :using :where :if]))))

   :drop-index
   (fn [sb q index]
     (-> sb
         (str+ "DROP INDEX")
         (emit-row q [:if-exists])
         (str+ " "  (cql-identifier index))))

   :drop-type
   (fn [sb q index]
     (-> sb
         (str+ "DROP TYPE")
         (emit-row q [:if-exists])
         (str+ " " (cql-identifier index))))

   :drop-table
   (fn [sb q table]
     (-> sb
         (str+ "DROP TABLE")
         (emit-row q [:if-exists])
         (str+ " " (cql-identifier table))))

   :drop-column-family
   (fn [sb q cf]
     (-> sb
         (str+ "DROP COLUMNFAMILY" )
         (emit-row q [:if-exists])
         (str+ " " (cql-identifier cf))))

   :drop-keyspace
   (fn [sb q keyspace]
     (-> (str+ sb "DROP KEYSPACE" )
         (emit-row q [:if-exists])
         (str+ " " (cql-identifier keyspace))))

   :use-keyspace
   (fn [sb q ks]
     (str+ sb "USE " (cql-identifier ks)))

   :truncate
   (fn [sb q ks]
     (str+ sb "TRUNCATE " (cql-identifier ks)))

   :grant
   (fn [sb q perm]
     (-> sb
         (str+ "GRANT ")
         (emit-row (assoc q :perm perm)
                   [:perm :resource :user])))

   :revoke
   (fn [sb q perm]
     (-> sb
         (str+ "REVOKE ")
         (emit-row (assoc q :perm perm) [:perm :resource :user])))

   :create-index
   (fn [sb {:keys [custom with]
         :as q}
        column]
     (-> (str+ sb "CREATE")
         (cond-> custom (str+ " CUSTOM"))
         (str+ " INDEX")
         (emit-row q [:if-exists :index-name :on])
         (str+ " " (wrap-parens (cql-identifier column)))
         (cond-> (and custom with)
           ((emit :with) q with))))

   :create-trigger
   (fn [sb {:keys [table using] :as q} name]
     (-> sb
         (str+  "CREATE TRIGGER " (cql-identifier name))
         (emit-row q [:on :using])))

   :drop-trigger
   (fn [sb q name]
     (-> sb
         (str+ "DROP TRIGGER " (cql-identifier name))
         (emit-row q [:on])))

   :create-user
   (fn [sb q user]
     (-> sb
         (str+ "CREATE USER " (cql-identifier user))
         (emit-row q [:password :superuser])))

   :alter-user
   (fn [sb q user]
     (-> sb
         (str+ "ALTER USER " (cql-identifier user))
         (emit-row q [:password :superuser])))

   :drop-user
   (fn [sb q user]
     (-> sb
         (str+ "DROP USER " (cql-identifier user)
               ;; (when-let [exists (emit-row q [:if-exists])]
               ;;   (str " " exists))
               ;; FIXME
               )))

   :list-users
   (fn [sb q _] (str+ sb "LIST USERS"))

   :perm
   (fn [sb q perm]
     (let [raw-perm (kw->c*const perm)]
       (-> sb
           (str+ "PERMISSION")
           (cond-> (= "ALL" raw-perm) (str+ "S"))
           (str+ " " raw-perm))))

   :list-perm
   (fn [sb q perm]
     (-> sb
         (str+ "LIST ")
         (emit-row (assoc q :perm perm) [:perm :resource :user :recursive])))

   :create-table
   (fn [sb q table]
     (-> sb
         (str+ "CREATE TABLE")
         (emit-row (assoc q :table table) [:if-exists :table :column-definitions :with])))

   :create-type
   (fn [sb q type]
     (-> sb
         (str+ "CREATE TYPE")
         (emit-row (assoc q :type type) [:if-exists :type :column-definitions])))

   :alter-table
   (fn [sb q table]
     (-> sb
         (str+ "ALTER TABLE " (cql-identifier table))
         (emit-row q [:alter-column :add-column :rename-column :drop-column :with])))

   :alter-type
   (fn [sb q type]
     (-> sb
         (str+ "ALTER TYPE " (cql-identifier type))
         (emit-row q [:alter-column :add-column :rename-column :drop-column])))

   :alter-columnfamily
   (fn [sb q cf]
     (-> sb
         (str+ "ALTER COLUMNFAMILY " (cql-identifier cf))
         (emit-row q [:alter-column :add-column :rename-column :drop-column :with])))

   :alter-keyspace
   (fn [sb q ks]
     (-> sb
         (str+ "ALTER KEYSPACE " (cql-identifier ks))
         (emit-row q [:with])))

   :create-keyspace
   (fn [sb q ks]
     (-> sb
         (str+ "CREATE KEYSPACE")
         (emit-row (assoc q :ks ks) [:if-exists :ks :with])))

   :resource
   (fn [sb q resource]
     ((emit :on) sb q resource))

   :user
   (fn [sb q user]
     (cond
      (contains? q :list-perm)
      ((emit :of) sb q user)

      (contains? q :revoke)
      ((emit :from) sb q user)

      (contains? q :grant)
      ((emit :to) sb q user)))

   :on
   (fn [sb q on]
     (str+ sb " ON " (cql-identifier on)))

   :to
   (fn [sb q to]
     (str+ sb " TO " (cql-identifier to)))

   :of
   (fn [sb q on]
     (str+ sb " OF " (cql-identifier on)))

   :from
   (fn [sb q table]
     (str+ sb " FROM " (cql-identifier table)))

   :into
   (fn [sb q table]
     (str+ sb " INTO " (cql-identifier table)))

   :columns
   (fn [sb q columns]
     (-> sb
         (str+ (if (sequential? columns)
                 (cql-identifiers-join-comma columns)
                 (cql-identifier columns)))))

   :where
   (fn [sb q clauses]
     (str+ sb  " WHERE " (query-cond clauses)))

   :if
   (fn [sb q clauses]
     (str+ sb " IF " (query-cond clauses)))

   :if-exists
   (fn [sb q b]
     (str+ sb " IF" (if (not b) " NOT " " ") "EXISTS"))

   :order-by
   (fn [sb q columns]
     (->> columns
          (map (fn [col-values] ;; Values are a pair of col and order
                 (join-spaced (map cql-identifier col-values))))
          join-comma
          (str+ sb " ORDER BY ")))

   :primary-key
   (fn [sb q primary-key]
     (->> (if (sequential? primary-key)
            (->> primary-key
                 (map (fn [pk]
                        (if (sequential? pk)
                          (->  (cql-identifiers-join-comma pk)
                               wrap-parens)
                          (cql-identifier pk))))
                 join-comma)
            (cql-identifier primary-key))
          wrap-parens
          (str+ sb "PRIMARY KEY ")))

   :column-definitions
   (fn [sb q column-definitions]
     (->> column-definitions
          (mapv (fn [[k & xs]]
                  (if (identical? :primary-key k)
                    ((:primary-key emit) (StringBuilder.) q (first xs))
                    (join-spaced (map cql-identifier (cons k xs))))))
          join-comma
          wrap-parens
          (str+ sb " ")))

   :limit
   (fn [sb q limit]
     (str+ sb  " LIMIT " (cql-value limit)))

   :values
   (fn [sb q x]
     (str+ sb
           (wrap-parens
            (transduce (comp (map #(cql-identifier (first %)))
                             interpose-comma)
                       str-fold
                       x))
           " VALUES "
           (wrap-parens
            (transduce (comp (map #(cql-value (second %)))
                             interpose-comma)
                       str-fold
                       x))))

   :set-columns
   (fn [sb q values]
     (-> sb
         (str+ "SET "
               (transduce
                (comp
                 (map (fn [[k v]]
                        (if (and (sequential? v)
                                 (some operator? v))
                          (counter k v)
                          (format-eq (cql-identifier k)
                                     (cql-value v)))))
                 (interpose ", "))
                str-fold
                values))))

   :using
   (fn [sb q args]
     (-> sb
         (str+ " USING "
               (if (coll? args)
                 (->> args
                      (map (fn [[n value]]
                             (str (-> n name StringUtils/upperCase)
                                  " " (cql-value value))))
                      join-and)
                 (option-value args)))))

   :compact-storage
   (fn [sb q v]
     (when v (str+ sb "COMPACT STORAGE")))

   :allow-filtering
   (fn [sb q v]
     (when v (str+ sb " ALLOW FILTERING")))

   :alter-column
   (fn [sb q [identifier type]]
     (str+ sb
           " ALTER "
           (cql-identifier identifier)
            " TYPE "
            (cql-identifier type)))

   :rename-column
   (fn [sb q [old-name new-name]]
     (str+ sb
           " RENAME "
           (cql-identifier old-name)
           " TO "
           (cql-identifier new-name)))

   :add-column
   (fn [sb q [identifier type]]
     (str+ sb
           " ADD "
           (cql-identifier identifier)
           " "
           (cql-identifier type)))

   :drop-column
   (fn [sb q identifier]
     (str+ sb " DROP " (cql-identifier identifier)))

   :clustering-order
   (fn [sb q columns]
     (->> columns
          (map (fn [col-values] ;; Values are a pair of col and order
                 (join-spaced (map cql-identifier col-values))))
          join-comma
          wrap-parens
          (str+ sb "CLUSTERING ORDER BY ")))

   :with
   (fn [sb q value-map]
     (->> (for [[k v] value-map]
            (if-let [with-entry (k emit)]
              (with-entry (StringBuilder.) q v)
              (format-eq (cql-identifier k)
                         (if (map? v)
                           (option-map v)
                           (option-value v)))))
          join-and
          (str+ sb " WITH ")))

   :password
   (fn [sb q pwd]
     ;; not sure if its a cql-id or cql-val
     (str+ sb " WITH PASSWORD " (cql-identifier pwd)))

   :superuser
   (fn [sb q superuser?]
     (str+ sb (if superuser? " SUPERUSER" " NOSUPERUSER")))

   :recursive
   (fn [sb q recursive]
     (when-not recursive (str+ sb " NORECURSIVE")))

   :index-column
   (fn [sb q index-column]
     (str+ sb (wrap-parens (cql-identifier index-column))))

   ;; :batch
   ;; (fn [sb {:keys [logged counter]
   ;;          :as q} queries]
   ;;   (-> sb
   ;;       (str+ "BEGIN")
   ;;       (cond->
   ;;           (not logged) (str+ " UNLOGGED")
   ;;           counter (str+ " COUNTER"))
   ;;       (str+ " BATCH ")
   ;;       (as-> (:using q) using
   ;;         (cond-> using
   ;;           (str+ ((emit :using) q using) " \n"))))
   ;;   (->> queries
   ;;        (remove nil?)
   ;;        (map emit-query)
   ;;        join-lf)

   ;;   (str+ sb  "BEGIN"
   ;;        (when-not logged " UNLOGGED")
   ;;        (when counter " COUNTER")
   ;;        " BATCH "
   ;;        (when-let [using (:using q)]
   ;;          (str ((emit :using) q using) " \n"))
   ;;        (->> queries
   ;;             (remove nil?)
   ;;             (map (partial emit-query sb))
   ;;             join-lf)
   ;;        "\n APPLY BATCH"))

   :queries
   (fn [sb q queries]
     (str+ sb "\n" (join-lf (map emit-query queries)) "\n"))
  })

(def emit-catch-all (fn [sb q x] (str+ sb " " (cql-identifier x))))

(def entry-clauses #{:select :insert :update :delete :use-keyspace :truncate
                     :drop-index :drop-type :drop-table :drop-keyspace :drop-columnfamily
                     :create-index :create-trigger :drop-trigger :grant :revoke
                     :create-user :alter-user :drop-user :list-users :list-perm
                     :batch :create-table :alter-table :alter-columnfamily
                     :alter-keyspace :create-keyspace :create-type :alter-type})

(defn find-entry-clause
  "Finds entry point key from query map"
  [qmap]
  (some entry-clauses (keys qmap)))

(defn emit-row
  [sb row template]
  (run! (fn [token]
          (when (contains? row token)
            ((get emit token emit-catch-all) sb row (token row))))
        template)
  sb)

;; (prn (->raw {:select "foo" :where [[:bar (CQLFn. "now" [])]]}))
;; (dotimes [_ 5 ]
;;   (time (dotimes [i 1]
;;           (->raw {:select "foo"}))))

(defn emit-query [sb query]
  (let [entry-point (find-entry-clause query)]
    (str (terminate ((emit entry-point) sb query (entry-point query))))))

(defn ->raw
  "Compiles a hayt query into its raw/string value"
  [query]
  (emit-query (StringBuilder.) query))


           ;; (->raw {:select "foo" :columns :* :where [[:bar "baz"]]})

;; (dotimes [_ 5 ]
;;   (time (dotimes [i 1000000]
;;            (->raw {:select "foo" :columns :* :where [[:bar "baz"]]}))))
