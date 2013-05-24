(ns qbits.hayt
  (:require
   [qbits.hayt.utils :as utils]
   [qbits.hayt.cql :as cql]))

(def ->raw
  "Compiles a hayt query into its raw string value"
  cql/->raw)

(def ->prepared
  "Compiles a hayt query into a vector composed of the prepared string
  query and a vector of parameters."
  cql/->prepared)

(doseq [module '(dsl fns utils)]
  (let [ns-name (symbol (str "qbits.hayt." module))]
    (utils/alias-ns ns-name)))
