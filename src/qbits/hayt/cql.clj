(ns qbits.hayt.cql
  "http://cassandra.apache.org/doc/cql3/CQL.html"
  (:require [clojure.string :as string]))

(declare emit-query)
(def ^:dynamic *param-stack*)
(def ^:dynamic *prepared-statement* true)

;; this has to be an atom, we cannot bash a transient in place (we
;; could but it's marked as sin in the docs ("an implementation detail")
(defmacro set-param!
  [x]
  `(if *prepared-statement*
     (do (swap! *param-stack* conj ~x)
         "?")
     ~x))

(def join-and #(string/join " AND " %))
(def join-spaced #(string/join " " %))
(def join-coma #(string/join ", " %))
(def join-lf #(string/join "\n" %))
(def format-eq (partial format "%s = %s"))
(def format-kv (partial format "%s : %s"))
(def quote-string #(str \' (string/escape % {\" "\""}) \'))
(def wrap-parens #(str "(" % ")"))
(def terminate #(str % ";"))

(defprotocol CQLEntities
  (cql-identifier [x] "table names etc, maybe it's too paranoid, but this also
                    allows their parameterisation")
  (cql-value [x] "Encodes a value for query consumption, pushing
                  parameters to a separate stack, and replacing its
                  value with a placeholder, the placeholder can be
                  specified and be either %s or ?, allowing use
                  with clojure.core/format for raw queries or as
                  prepared statements"))

(extend-protocol CQLEntities

  String
  (cql-identifier [x] (set-param! (name x)))
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (quote-string x)))

  clojure.lang.Keyword
  (cql-identifier [x] (cql-identifier (name x)))
  (cql-value [x] (cql-value (name x)))

  ;; collections are just for cassandra collection types, not to
  ;; generate query parts, ex in where clause
  clojure.lang.IPersistentSet
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (str "{" (join-coma (map cql-value x)) "}")))

  clojure.lang.IPersistentMap
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (map (fn [[k v]]
             (format-kv (cql-value k) (cql-value v)))
           x)))

  clojure.lang.Sequential
  (cql-value [x]
    (if *prepared-statement*
      (set-param! x)
      (str "[" (join-coma (map cql-value x)) "]")))

  Object
  (cql-identifier [x] (set-param! x))
  (cql-value [x] (set-param! x)))

(def operators {= "="
                > ">"
                < "<"
                <= "<="
                >= ">="
                + "+"
                - "-"})

(defn where-sequential-entry [column [op value]]
  (let [col-name (cql-identifier column)]
    (cond
      (= :in op)
      (str col-name
           " IN "
           (wrap-parens (join-coma (map cql-value value))))

      (fn? op)
      (str col-name
           " " (operators op) " "
           (cql-value value))

      (keyword? op)
      (str col-name
           " " (name op) " "
           (cql-value value)))))

(defn counter [column [op value]]
  (format-eq (cql-identifier column)
             ;; we cannot cache the col-name value, since there is a
             ;; stack update behind this call
             (join-spaced [(cql-identifier column)
                           (operators op)
                           (cql-value value)])))

(def emit
  {:columns
   (fn [q fields]
     (if (empty? fields)
       "*"
       (join-coma (map cql-identifier fields))))

   :where
   (fn [q clauses]
     (->> clauses
          (map (fn [[k v]]
                 (if (sequential? v) ;; sequence, we do the complex thing first
                   (where-sequential-entry k v)
                   ;; else we just append if its a simple map val
                   (format-eq (cql-identifier k)
                              (cql-value v)))))
          join-and
          (str "WHERE ")))

   :order-by
   (fn [q columns]
     (->> columns
          (map (fn [col-values] ;; values are a pair of col name and order (DESC, ASC)
                 (join-spaced (map cql-identifier col-values))))
          join-coma
          (str "ORDER BY ")))

   :limit
   (fn [q limit]
     (assert (number? limit) "Limit only accepts numbers")
     (str "LIMIT " limit))

   :values
   (fn [q values-map]
     (let [columns (keys values-map)
           values (vals values-map)]
       (str (wrap-parens (join-coma (map cql-identifier columns)))
            " VALUES "
            (wrap-parens (join-coma (map cql-value values))))))

   :set
   (fn [q values]
     (->> (map (fn [[k v]]
                 ;; counter (we need to support maps/set/list updates
                 ;; too, so this is kind of a hack now)
                 (if (vector? v)
                   (counter k v)
                   (format-eq (cql-identifier k) (cql-value v))))
               values)
          join-coma
          (str "SET ")))

   :using
   (fn [q args]
     (->> (for [[n value] (partition 2 args)]
            (str (-> n name string/upper-case)
                 " " (cql-identifier value)))
          join-and
          (str "USING ")))

   :with
   (fn [q value-map]
     (->> (for [[k v] value-map]
            (format-eq (cql-identifier k)
                       (cql-value v)))
          join-and
          (str "WITH ")))

   :queries
   (fn [q queries]
     (let [subqs (map (fn [{:keys [template query]}]
                        (emit-query query template))
                      queries)]
       (if *prepared-statement*
         [(join-lf subqs) @*param-stack*])
       (join-lf subqs)))})

(def emit-catch-all (fn [q x] (cql-identifier x)))

(defn emit-query [query template]
  (->> (map (fn [token]
              (if (string? token)
                token
                (when-let [context (token query)]
                  ((get emit token emit-catch-all) query context))))
            template)
       (filter identity)
       join-spaced
       terminate))

(defn apply-cql
  [query template]
  (binding [*prepared-statement* false]
    (emit-query query template)))

(defn apply-prepared
  [query template]
  (binding [*param-stack* (atom [])]
    [(emit-query query template)
     @*param-stack*]))
