(ns qbits.hayt.utils
  (:require [qbits.hayt.cql :as cql]))

(def ^:no-doc native-types
  [:ascii
   :bigint
   :blob
   :boolean
   :counter
   :decimal
   :double
   :float
   :inet
   :int
   :text
   :timestamp
   :timeuuid
   :uuid
   :varchar
   :varint])

;; Sugar for collection types

(defn ^:no-doc coll-type
  "Helps with the generation of Collection types definitions.
Takes a CQL type as keyword and it's arguments: ex (coll-type :map :int :uuid).
The possible collection types are :map, :list and :set."
  [t & spec]
  (keyword (format "%s<%s>"
                   (name t)
                   (cql/join-comma (map name spec)))))

(def map-type
  "Generates a map type definition, takes 2 arguments, for key and
  value types"
  (partial coll-type :map))

(def list-type
  "Generates a list type definition, takes a single argument
  indicating the list elements type"
  (partial coll-type :list))

(def set-type
  "Generates a set type definition, takes a single argument indicating
  the set elements type"
  (partial coll-type :set))
