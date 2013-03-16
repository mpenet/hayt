(ns qbits.hayt
  (:require
   [clojure.core.typed :as t]
   [useful.ns :as uns]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.types :refer [HaytQuery HaytClause]]))

(def ->raw cql/->raw)
(def ->prepared cql/->prepared)

(t/ann q-> [HaytQuery HaytClause * -> HaytQuery])
(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (into q clauses))

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))
