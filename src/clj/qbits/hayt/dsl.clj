(ns qbits.hayt.dsl
  (:require [flatland.useful.ns :as uns]))

(doseq [module '(statement clause)]
  (uns/alias-ns (symbol (str "qbits.hayt.dsl." module))))
