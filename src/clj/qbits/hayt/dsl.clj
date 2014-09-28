(ns qbits.hayt.dsl
  (:refer-clojure :exclude [update])
  (:require [qbits.commons.ns :as uns]))

(doseq [module '(statement clause)]
  (uns/alias-ns (symbol (str "qbits.hayt.dsl." module))))
