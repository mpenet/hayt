(ns qbits.hayt
  "This namespace contains aliases for qbits.dsl.*, qbits.fns and qbits.utils"
  (:refer-clojure :exclude [update])
  (:require
   [qbits.commons.ns :as uns]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))
