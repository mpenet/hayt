(ns qbits.hayt.cql
  "CQL3 ref: http://cassandra.apache.org/doc/cql3/CQL.html or
https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#functions
for a more up to date version "
  (:require [clojure.string :as string]))

(declare emit-query)
(def ^:dynamic *param-stack*)
(def ^:dynamic *prepared-statement* false)

(defn template [q] (-> q meta :template))

;; Wraps a CQL function (a template to clj.core/format and its
;; argument for later encoding.
(defrecord CQLFn [value template])

(defn set-param!
  [x]
  (if *prepared-statement*
    (do (swap! *param-stack* conj x)
        "?")
    x))

(def join-and #(string/join " AND " %))
(def join-spaced #(string/join " " %))
(def join-comma #(string/join ", " %))
(def join-lf #(string/join "\n" %))
(def format-eq #(format "%s = %s" %1 %2))
(def format-kv #(format "%s : %s"  %1 %2))
(def quote-string #(str "'" (string/replace % "'" "''") \'))
(def dquote-string #(str "\"" (string/replace % "\" " "\"\"") "\""))
(def wrap-parens #(str "(" % ")"))
(def wrap-brackets #(str "{" % "}"))
(def wrap-sqbrackets #(str "[" % "]"))
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
    (if *prepared-statement*
      (set-param! x)
      (quote-string x)))

  clojure.lang.Keyword
  (cql-identifier [x] (name x))
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (cql-value (name x))))

  ;; Collections are just for cassandra collection types, not to
  ;; generate query parts
  clojure.lang.IPersistentSet
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (->> (map cql-value x)
           join-comma
           wrap-brackets)))

  clojure.lang.IPersistentMap
  (cql-identifier [x]
    (let [[coll k] (first x) ]
      ;; handles foo['bar'] lookups
      (str (cql-identifier coll)
           (wrap-sqbrackets (cql-value k)))))
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (->> x
           (map (fn [[k v]]
                  (format-kv (cql-value k)
                             (cql-value v))))
           join-comma
           wrap-brackets)))

  clojure.lang.Sequential
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (->> (map cql-value x)
           join-comma
           wrap-sqbrackets)))

  ;; CQL Function are always safe, their arguments might not be though
  CQLFn
  (cql-identifier [{:keys [value template]}]
    (if template
      (format template (cql-identifier value))
      value))
  (cql-value [{:keys [value template]}]
    (if template
      (format template (cql-value value))
      value))

  nil
  (cql-value [x]
    (throw (UnsupportedOperationException.
            "'null' parameters are not allowed since CQL3 does
not (yet) supports them. See
https://issues.apache.org/jira/browse/CASSANDRA-3783")))

  Object
  (cql-identifier [x] x)
  (cql-value [x] (set-param! x)))

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
  (if (number? x)
    x
    (quote-string (name x))))

(defn option-map [m]
  (->> m
       (map (fn [[k v]]
              (format-kv (quote-string (name k))
                         (option-value v))))
       join-comma
       wrap-brackets))


(defn format-column-definition
  [[k v]]
  (join-spaced
   [(cql-identifier k)
    (cql-identifier v)]))

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
  {:columns
   (fn [q columns]
     (if (empty? columns)
       "*"
       (join-comma (map cql-identifier columns))))

   :where
   (fn [q clauses]
     (->> (partition 2 clauses)
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
            (map cql-identifier primary-key)
            (cql-identifier primary-key))
          join-comma
          wrap-parens
          (str "PRIMARY KEY ")))

   :column-definitions
   (fn [q {:keys [primary-key] :as column-definitions}]
     (-> (mapv format-column-definition
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
     (->> (partition 2 args)
          (map (fn [[n value]]
                 (str (-> n name string/upper-case)
                      " " (cql-identifier value))))
          join-and
          (str "USING ")))

   :compact-storage (constantly "COMPACT STORAGE")

   :alter
   (fn [q [identifier type]]
     (format "ALTER %s TYPE %s"
             (cql-identifier identifier)
             (cql-identifier type)))

   :add
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

   :index-column
   (fn [q index-column]
     (wrap-parens (cql-identifier index-column)))

   :queries
   (fn [q queries]
     (->> (let [subqs (join-lf (map emit-query queries))]
            (if *prepared-statement*
              [subqs @*param-stack*])
            subqs)
          (format "\n%s\n")))})

(def emit-catch-all (fn [q x] (cql-identifier x)))

(defn emit-query [query]
  (->> (template query)
       (map (fn [token]
              (if (string? token)
                token
                (when-let [context (token query)]
                  ((get emit token emit-catch-all) query context)))))
       (filter identity)
       join-spaced
       terminate))
