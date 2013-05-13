(ns qbits.hayt.dsl
  (:require [flatland.useful.ns :as uns]))

(doseq [module '(verb clause)]
  (uns/alias-ns (symbol (str "qbits.hayt.dsl." module))))
