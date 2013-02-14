(ns qbits.hayt.cql
  "http://cassandra.apache.org/doc/cql3/CQL.html"
  (:require [clojure.string :as string]))

(def ^:dynamic *param-stack*)
(def ^:dynamic *param-placeholder* "%s")

;; this has to be an atom, we cannot bash a transient in place (we
;; could but it's marked as sin in the docs :)
(defmacro set-param!
  [x]
  `(do  (swap! *param-stack* conj ~x)
        *param-placeholder*))

;; string manip helpers
(def join-and (partial string/join " and "))
(def join-spaced (partial string/join " "))
(def join-coma (partial string/join ", "))
(def format-eq (partial format "%s = %s"))
(def format-kv (partial format "%s : %s"))
(def quote-string #(str \' (string/escape % {\" "\""}) \'))
(def terminate #(str % ";"))

(defprotocol PEncodable
  (encode-name [x] "table names etc")
  (encode-value [x] "Encodes a value for query consumption, pushing
                     parameters to a separate stack, and replacing its value with a
                     placeholder, the placeholder can be specified and be either %s or ?,
                     allowing use with clojure.core/format or as a prepared statement"))

(extend-protocol PEncodable

  String
  (encode-name [x] (set-param! (name x)))
  (encode-value [x] (set-param! (quote-string x)))

  clojure.lang.Keyword
  (encode-name [x] (encode-name (name x)))
  (encode-value [x] (encode-name (name x)))

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

;; token   : the placeholder in the template
;; query   : complete query in case we need refs to other fields
;; context : specific context we are dealing with
;; we dont pad with space these return values (thus avoid later triming)
(defmulti emit (fn [token query context] token))

(defmethod emit :columns
  [_ q fields]
  (if (empty? fields)
    "*"
    (join-coma (map encode-name fields))))

(defmethod emit :where
  [_ q expressions]

  )

(defmethod emit :order-by
  [_ q columns]
  (->> columns
       (map (fn [col-values] ;; values are a pair of col name and order (DESC, ASC)
              (join-spaced (map encode-name col-values))))
       join-coma
       (str "ORDER BY ")))

(defmethod emit :limit
  [_ q limit]
  (assert (number? limit) "Limit only accepts numbers")
  (str "LIMIT " limit))

(defmethod emit :using
  [_ q args]
  (->> (for [[n value] (partition 2 args)]
          (str (-> n name string/upper-case)
               " " (encode-name value)))
       join-and
       (str "USING ")))

(defmethod emit :with
  [_ value-map]
  (->> (for [[k v] value-map]
         (format-eq (encode-name k) (encode-value v)))
       join-and
       (str "WITH ")))

;; everything else is considered unsafe just define an emit for a
;; placeholder if you want to bypass this, stuff such as limit is
;; considered safe
(defmethod emit :default [_ q x] (encode-name x))

(defn apply-template
  [query template]
  (binding [*param-stack* (atom [])]
    [(->> (map (fn [token]
                 (if (string? token)
                   token
                   (when-let [context (token query)]
                     (emit token query context))))
               template)
          (filter identity)
          join-spaced
          terminate)
     @*param-stack*]))
