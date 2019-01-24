(ns qbits.hayt.dsl
  (:refer-clojure :exclude [update group-by])
  (:require [qbits.hayt.ns :as uns]))

(doseq [module '(statement clause)]
  (uns/alias-ns (symbol (str "qbits.hayt.dsl." module))))
