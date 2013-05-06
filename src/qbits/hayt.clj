(ns qbits.hayt
  (:require
   [flatland.useful.ns :as uns]
   [qbits.hayt.cql :as cql]))

(def ->raw cql/->raw)
(def ->prepared cql/->prepared)

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))
