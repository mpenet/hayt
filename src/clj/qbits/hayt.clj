(ns qbits.hayt
  "This namespace contains aliases for qbits.dsl.*, qbits.fns and qbits.utils"
  (:refer-clojure :exclude [update])
  (:require
   [qbits.hayt.ns :as uns]
   [qbits.commons.jvm :refer [compile-if-ns-exists]]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))

(compile-if-ns-exists
 qbits.alia
 (do
   (require '[qbits.alia :as alia])
   ;; try to support both v3 and v4 of alia
   (if (-> alia/PStatement :sigs :query->statement :arglists first count (= 3))
     (try
       (eval '(extend-protocol alia/PStatement
                clojure.lang.APersistentMap
                (query->statement [q values codec]
                  (alia/query->statement (->raw q) values codec))))
       (catch Exception _ nil))
     (try
       (eval '(extend-protocol alia/PStatement
                clojure.lang.APersistentMap
                (query->statement [q values]
                  (alia/query->statement (->raw q) values))))
       (catch Exception _ nil)))))
