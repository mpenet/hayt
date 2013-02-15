(ns qbits.hayt
  (:require [qbits.hayt.cql :as cql]))

(defn as-cql [query]
  (binding [qbits.hayt.cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn as-prepared [query]
  (binding [cql/*param-stack* (atom [])]
    [(cql/emit-query query)
     @cql/*param-stack*]))

(defn query
  [template query-map]
  (vary-meta query-map assoc :template template))

(defn select
  ""
  [table]
  (query
   ["SELECT" :columns "FROM" :table :where :order-by :limit]
   {:table table
    :columns []}))

(defn insert
  ""
  [table]
  (query ["INSERT INTO" :table :values :using]
         {:table table}))

(defn update
  ""
  [table]
  (query ["UPDATE" :table :using :set-fields :where]
         {:table table}))

(defn delete
  ""
  [table]
  (query ["DELETE" :columns "FROM" :table :using :where]
         {:table table
          :columns []}))

(defn truncate
  ""
  [table]
  (query ["TRUNCATE" :table]
          {:table table}))

(defn drop-keyspace
  ""
  [keyspace]
  (query ["DROP KEYSPACE" :keyspace]
         {:keyspace keyspace}))

(defn drop-table
  ""
  [table]
  (query ["DROP TABLE" :table]
         {:table table}))

(defn drop-index
  ""
  [index]
  (query ["DROP INDEX" :index]
         {:index index}))

(defn create-index
  ""
  [table column]
  (query ["CREATE INDEX" :index-name "ON" :table "(" :column ")"]
         {:table table :column column}))


(defn batch
  ""
  [& queries]
  (query ["BATCH" :using "\n" :queries  "\nAPPLY BATCH"]
         {:queries queries}))


;; clauses

(defn columns
  ""
  [q & columns]
  (assoc q :columns columns))

(defn using
  ""
  [q & args]
  (assoc q :using args))

(defn limit
  ""
  [q n]
  (assoc q :limit n))

(defn order-by
  ""
  [q & fields]
  (assoc q :order-by fields))

(defn where
  ""
  [q args]
  (assoc q :where args))

(defn values
  ""
  [q values]
  (assoc q :values values))

(defn set-fields
  ""
  [q values]
  (assoc q :set-fields values))

;; (defn def-cols [q values]
;;   (update-in q [:query :defs] merge values))

;; (defn def-pk [q & values]
;;   (assoc-in q [:query :defs :pk] values))

(defn with
  ""
  [q values]
  (assoc q :with values))

(defn index-name
  ""
  [q value]
  (assoc q :index-name value))
