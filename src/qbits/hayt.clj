(ns qbits.hayt
  (:require
   [clojure.core.typed :as t]
   [useful.ns :as uns]
   [qbits.hayt.cql :as cql]))

(t/ann ->raw [(clojure.lang.APersistentMap Any Any) -> String])
(def ->raw cql/->raw)

(t/ann ->prepared [(clojure.lang.APersistentMap Any Any) -> '[String '[Any]]])
(def ->prepared cql/->prepared)

(->raw {:select :users :columns :*})



(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (into q clauses))

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))


(t/check-ns 'qbits.hayt)
