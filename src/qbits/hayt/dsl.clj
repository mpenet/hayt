(ns qbits.hayt.dsl
  (:require [qbits.hayt.utils :as utils]))

(doseq [module '(statement clause)]
  (let [ns-name (symbol (str "qbits.hayt.dsl." module))]
    (utils/alias-ns ns-name)))
