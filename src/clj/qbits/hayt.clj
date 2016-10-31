(ns qbits.hayt
  "This namespace contains aliases for qbits.dsl.*, qbits.fns and qbits.utils"
  (:refer-clojure :exclude [update])
  (:require
   [qbits.commons.ns :as uns]
   [qbits.commons.jvm :refer [compile-if-ns-exists]]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))

(compile-if-ns-exists qbits.alia
 (do (require '[qbits.alia :as alia])
     (extend-protocol alia/PStatement
       clojure.lang.APersistentMap
       (query->statement [q values codec]
         (alia/query->statement (->raw q) values codec)))))
