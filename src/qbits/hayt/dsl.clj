(ns qbits.hayt.dsl
  (:require [qbits.hayt.utils :as utils]))

(doseq [module '(statement clause)]
  (utils/alias-ns (symbol (str "qbits.hayt.dsl." module))))
