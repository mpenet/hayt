(ns qbits.hayt
  (:require
   [clojure.core.typed :as t]
   [useful.ns :as uns]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.types :refer [HaytQuery HaytClause]]))

(t/ann ->raw [HaytQuery -> String])
(def ->raw cql/->raw)

(t/ann ->prepared [HaytQuery -> '[String '[Any]]])
(def ->prepared cql/->prepared)

;; (prn (t/cf (->raw {:select :users :columns :*})))

(t/ann q-> [HaytQuery HaytClause * -> String])
(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (into q clauses))

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))
