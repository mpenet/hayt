(ns qbits.hayt.types
  (:require
   [clojure.core.typed :as t])
  (:import
   [clojure.lang
    Keyword
    APersistentMap
    Sequential]))

(t/def-alias MaybeSequential (t/Option Sequential))

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

(t/def-alias CQLPermission (U ':all ':alter ':authorize ':create ':drop ':modify
                              ':select))

(t/def-alias CQLIdentifier (U Keyword String))


;; could make sense to define a type for possible parameterized values
;; allowing to type check qbits.hayt.cql/maybe-parameterize! for instance
