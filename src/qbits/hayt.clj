(ns qbits.hayt
  (:require
   [useful.ns :as uns]
   [qbits.hayt.cql :as cql]))

(def ->raw cql/->raw)
(def ->prepared cql/->prepared)

(defn q->
  "Allows query composition, extending an existing query with new
  clauses"
  [q & clauses]
  (into q clauses))

(doseq [module '(dsl fns utils)]
  (uns/alias-ns (symbol (str "qbits.hayt." module))))

;; (dotimes [_ 5 ]
;;   (time (dotimes [i 100000]
;;           (->raw (q-> (select :*)
;;                       (from :foo)
;;                       (where {:foo :bar
;;                               :moo [> 3]
;;                               :meh [:> 4]
;;                               :baz [:in [5 6 7]]}))))))

;; "Elapsed time: 1231.696198 msecs"
;; "Elapsed time: 1187.504541 msecs"
;; "Elapsed time: 1144.763966 msecs"
;; "Elapsed time: 1157.423996 msecs"
;; "Elapsed time: 1153.117316 msecs"
;; "Elapsed time: 1267.216894 msecs"
;; "Elapsed time: 1197.197213 msecs"
;; "Elapsed time: 1147.885969 msecs"
;; "Elapsed time: 1145.004844 msecs"
;; "Elapsed time: 1152.210611 msecs"
