(ns qbits.hayt
  (:require
   [useful.ns :as uns]
   [qbits.hayt.cql :as cql]))

(def ->raw cql/->raw)
(def ->prepared cql/->prepared)

(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (into q clauses))

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))
