(ns qbits.hayt.types
  (:require
   [clojure.core.typed :as t])
  (:import
   [clojure.lang APersistentMap]))

(t/def-alias HaytQuery (APersistentMap Any Any))
(t/def-alias HaytClause (APersistentMap Any Any))
(t/def-alias CompiledQuery String)

(t/def-alias C*CollType (U ':list ':map ':set))

(t/def-alias C*Type (U ':ascii
                       ':bigint
                       ':blob
                       ':boolean
                       ':counter
                       ':decimal
                       ':double
                       ':float
                       ':inet
                       ':int
                       ':text
                       ':timestamp
                       ':timeuuid
                       ':uuid
                       ':varchar
                       ':varint))
