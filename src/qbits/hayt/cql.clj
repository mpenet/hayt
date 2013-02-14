(ns qbits.hayt.cql
  "http://cassandra.apache.org/doc/cql3/CQL.html"
  (:require [clojure.string :as string]))

;; not this is just examples for now, some stuff can be optmized to
;; reduce the number of iterations here and there.

(def ^:dynamic *param-stack*)

;; we'll just overwrite this for prepared statements, yeah it's a dyn
;; var, sucks, maybe it's better to do that from an argument
(def ^:dynamic *param-placeholder* "%s")

(def ^:dynamic *escape-values* true)

;; this has to be an atom, we cannot bash a transient in place (we
;; could but it's marked as sin in the docs ("an implementation detail")
(defmacro set-param!
  [x]
  `(do  (swap! *param-stack* conj ~x)
        *param-placeholder*))

;; string manip helpers
(def join-and (partial string/join " AND "))
(def join-spaced (partial string/join " "))
(def join-coma (partial string/join ", "))
(def format-eq (partial format "%s = %s"))
(def format-kv (partial format "%s : %s"))
(def quote-string #(str \' (string/escape % {\" "\""}) \'))
(def wrap-parens #(str "(" % ")"))
(def terminate #(str % ";"))

;; about quoting and escaping stuff: java-driver expects raw values on
;; prepared-statements .bind, doing stuff such as string escaping is
;; not necessary welcomed in this case. Maybe something to handle from
;; an option again. not sure yet.

(defprotocol PEncodable
  (encode-name [x] "table names etc, maybe it's too paranoid, but this also
                    allows their parameterisation")
  (encode-value [x] "Encodes a value for query consumption, pushing
                     parameters to a separate stack, and replacing its
                     value with a placeholder, the placeholder can be
                     specified and be either %s or ?, allowing use
                     with clojure.core/format for raw queries or as
                     prepared statements"))

(extend-protocol PEncodable

  String
  (encode-name [x] (set-param! (name x)))
  (encode-value [x] (set-param! (if *escape-values*
                                  (quote-string x)
                                  x)))

  clojure.lang.Keyword
  (encode-name [x] (encode-name (name x)))
  (encode-value [x] (encode-name (name x)))

  ;; collections are just for cassandra collection types, not to
  ;; generate query parts, ex in where clause
  clojure.lang.IPersistentSet
  (encode-value [x]
    (str "{" (join-coma (map encode-value x)) "}"))

  clojure.lang.IPersistentMap
  (encode-value [x]
    (->> (map (fn [[k v]]
                (format-kv (encode-value k) (encode-value v)))
              x)
         join-coma
         #(str "{" % "}")))

  clojure.lang.Sequential
  (encode-value [x]
    (str "[" (join-coma (map encode-value x)) "]"))

  Object
  (encode-name [x] (set-param! x))
  (encode-value [x] (set-param! x)))

(def operators {= "="
                > ">"
                < "<"
                <= "<="
                >= ">="})

(defn where-sequential-entry [column [op value]]
  (let [col-name (encode-name column)]
    (cond
      (= :in op)
      (str col-name
           " IN "
           (wrap-parens (join-coma (map encode-value value))))

      (fn? op)
      (str col-name
           " " (operators op) " "
           (encode-value value))

      (keyword? op)
      (str col-name
           " " (name op) " "
           (encode-value value)))))

(defn counter [column [op value]]
  (format-eq (encode-name column)
             ;; we cannot cache the col-name value, since there is a
             ;; stack update behind this call
             (join-spaced [(encode-name column)
                           (operators op)
                           (encode-value value)])))

(def emit
  {:columns
   (fn [q fields]
     (if (empty? fields)
       "*"
       (join-coma (map encode-name fields))))

   :where
   (fn [q clauses]
     (->> clauses
          (map (fn [[k v]]
                 (if (sequential? v) ;; sequence, we do the complex thing first
                   (where-sequential-entry k v)
                   ;; else we just append if its a simple map val
                   (format-eq (encode-name k)
                              (encode-value v)))))
          join-and
          (str "WHERE ")))

   :order-by
   (fn [q columns]
     (->> columns
          (map (fn [col-values] ;; values are a pair of col name and order (DESC, ASC)
                 (join-spaced (map encode-name col-values))))
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
       (str (wrap-parens (join-coma (map encode-name columns)))
            " VALUES "
            (wrap-parens (join-coma (map encode-value values))))))

   :set
   (fn [q values]
     (->> (map (fn [[k v]]
                 ;; counter (we need to support maps/set/list updates
                 ;; too, so this is kind of a hack now)
                 (if (vector? v)
                   (counter k v)
                   (format-eq (encode-name k) (encode-value v))))
               values)
          join-coma
          (str "SET ")))

   :using
   (fn [q args]
     (->> (for [[n value] (partition 2 args)]
            (str (-> n name string/upper-case)
                 " " (encode-name value)))
          join-and
          (str "USING ")))

   :with
   (fn [q value-map]
     (->> (for [[k v] value-map]
            (format-eq (encode-name k) (encode-value v)))
          join-and
          (str "WITH ")))})

(def emit-catch-all (fn [q x] (encode-name x)))

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
