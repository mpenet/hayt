(ns qbits.hayt
  (:require [qbits.hayt.cql :as cql]))

(defn as-cql [query]
  (binding [qbits.hayt.cql/*prepared-statement* false]
    (cql/emit-query query)))

(defn as-prepared [query]
  (binding [cql/*param-stack* (atom [])]
    [(cql/emit-query query)
     @cql/*param-stack*]))

(defn merge-clauses
  [q parts]
  (apply merge q parts))

(defn query
  [template query-map]
  (vary-meta query-map assoc :template template))

(defn select
  ""
  [table & clauses]
  (query ["SELECT" :columns "FROM" :table :where :order-by :limit]
         (merge-clauses {:table table
                         :columns []}
                        clauses)))

(defn insert
  ""
  [table & clauses]
  (query ["INSERT INTO" :table :values :using]
         (merge-clauses {:table table}
                        clauses)))

(defn update
  ""
  [table & clauses]
  (query ["UPDATE" :table :using :set-fields :where]
         (merge-clauses {:table table}
                        clauses)))

(defn delete
  ""
  [table & clauses]
  (query ["DELETE" :columns "FROM" :table :using :where]
         (merge-clauses {:table table
                         :columns []}
                        clauses)))

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
  [table column & clauses]
  (query ["CREATE INDEX" :index-name "ON" :table "(" :column ")"]
         (merge-clauses {:table table
                         :column column}
                        clauses)))


(defn batch
  ""
  [& clauses]
  (query ["BATCH" :using "\n" :queries  "\nAPPLY BATCH"]
         (merge-clauses {} clauses)))


;; clauses

(defn columns
  ""
  [& columns]
  {:columns columns})

(defn using
  ""
  [& args]
  {:using args})

(defn limit
  ""
  [n]
  {:limit n})

(defn order-by
  ""
  [& fields]
  {:order-by fields})

(defn queries
  ""
  [& queries]
  {:queries queries})

(defn where
  ""
  [args]
  {:where args})

(defn values
  ""
  [values]
  {:values values})

(defn set-fields
  ""
  [values]
  {:set-fields values})

;; (defn def-cols [q values]
;;   (update-in q [:query :defs] merge values))

;; (defn def-pk [q & values]
;;   (assoc-in q [:query :defs :pk] values))

(defn with
  ""
  [values]
  {:with values})

(defn index-name
  ""
  [value]
  {:index-name value})

(defn q->
  [q & clauses]
  (merge-clauses q clauses))
