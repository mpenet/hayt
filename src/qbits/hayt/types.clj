(ns qbits.hayt.types
  (:require
   [clojure.core.typed :as t])
  (:import
   [clojure.lang APersistentMap]))

(t/def-alias HaytQuery (APersistentMap Any Any))
(t/def-alias HaytClause (APersistentMap Any Any))
(t/def-alias CompiledQuery String)

(t/def-alias C*CollType (U (Value :list)
                           (Value :map)
                           (Value :set)))

(t/def-alias C*Type (U (Value :ascii)
                       (Value :bigint)
                       (Value :blob)
                       (Value :boolean)
                       (Value :counter)
                       (Value :decimal)
                       (Value :double)
                       (Value :float)
                       (Value :inet)
                       (Value :int)
                       (Value :text)
                       (Value :timestamp)
                       (Value :timeuuid)
                       (Value :uuid)
                       (Value :varchar)
                       (Value :varint)))
