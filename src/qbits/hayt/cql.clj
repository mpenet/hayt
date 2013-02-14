(ns qbits.hayt.cql
  "http://cassandra.apache.org/doc/cql3/CQL.html"
  (:require [clojure.string :as string]))

;; not this is just examples for now, some stuff can be optmized to
;; reduce the number of iterations here and there.

(def ^:dynamic *param-stack*)

;; we'll just overwrite this for prepared statements, yeah it's a dyn
;; var, sucks, maybe it's better to do that from an argument
(def ^:dynamic *param-placeholder* "%s")
(def ^:dynamic *raw-values* true)

;; this has to be an atom, we cannot bash a transient in place (we
;; could but it's marked as sin in the docs ("an implementation detail")
(defmacro set-param!
  [x]
  `(do (swap! *param-stack* conj ~x)
       *param-placeholder*))

;; string manip helpers
(def join-and #(string/join " AND " %))
(def join-spaced #(string/join " " %))
(def join-coma #(string/join ", " %))
(def format-eq (partial format "%s = %s"))
(def format-kv (partial format "%s : %s"))
(def quote-string #(str \' (string/escape % {\" "\""}) \'))
(def wrap-parens #(str "(" % ")"))
(def terminate #(str % ";"))

;; about quoting and escaping stuff: java-driver expects raw values on
;; prepared-statements .bind, doing stuff such as string escaping is
;; not necessary welcomed in this case. Maybe something to handle from
;; an option again. not sure yet.

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
    (set-param! (if *raw-values*
                  x
                  (quote-string x))))

  clojure.lang.Keyword
  (cql-identifier [x] (cql-identifier (name x)))
  (cql-value [x] (cql-identifier (name x)))

  ;; collections are just for cassandra collection types, not to
  ;; generate query parts, ex in where clause
  clojure.lang.IPersistentSet
  (cql-value [x]
    (if *raw-values*
        (set-param! x)
        (str "{" (join-coma (map cql-value x)) "}")))

  clojure.lang.IPersistentMap
  (cql-value [x]
    (if *raw-values*
      (set-param! x)
      (->> (map (fn [[k v]]
                  (format-kv (cql-value k) (cql-value v)))
                x)
           join-coma
           #(str "{" % "}"))))

  clojure.lang.Sequential
  (cql-value [x]
    (if *raw-values*
      (set-param! x)
      (str "[" (join-coma (map cql-value x)) "]")))

  Object
  (cql-identifier [x] (set-param! x))
  (cql-value [x] (set-param! x)))

(def operators {= "="
                > ">"
                < "<"
                <= "<="
                >= ">="})

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
          (str "WITH ")))})

(def emit-catch-all (fn [q x] (cql-identifier x)))

;; everything else is considered unsafe just define an emit for a
;; placeholder if you want to bypass this, stuff such as limit is
;; considered safe

(defn apply-template
  [query template]
  (binding [*param-stack* (atom [])]
    [(->> (map (fn [token]
                 (if (string? token)
                   token
                   (when-let [context (token query)]
                     ((get emit token emit-catch-all) query context))))
               template)
          (filter identity)
          join-spaced
          terminate)
     @*param-stack*]))
