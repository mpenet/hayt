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

(defn ^:no-doc complex-type
  "Helps with the generation of Collection types definitions.
Takes a CQL type as keyword and it's arguments: ex (coll-type :map :int :uuid).
The possible collection types are :map, :list and :set."
  [t & spec]
  (keyword (format "%s<%s>"
                   (name t)
                   (cql/join-comma (map name spec)))))

(def ^:no-doc ^:deprecated coll-type complete-type)

(def map-type
  "Generates a map type definition, takes 2 arguments, for key and
  value types"
  (partial complex-type :map))

(def list-type
  "Generates a list type definition, takes a single argument
  indicating the list elements type"
  (partial complex-type :list))

(def set-type
  "Generates a set type definition, takes a single argument indicating
  the set elements type"
  (partial complex-type :set))

(def tuple-type
  "Generates a tuple type definition, takes n arguments"
  (partial complex-type :tuple))

(def ?
  "? can be used as a query value to mark a prepared statement value
ex:    (select :foo
           (where [[> :foo ?]
                   [< :foo 2]]]))"
  (cql/->CQLRaw "?"))

(def in
  "`in` can be used as a query value to mark IN in where clause
ex:    (select :foo (where [[in :foo  [1 2 3]]]))"
:in)

(def contains
  "`contains` can be used as a query value to mark CONTAINS in where clause
ex:    (select :foo (where [[contains :foo  ...]]]))"
  cql/contains)

(def contains-key
  "`contains-key` can be used as a query value to mark CONTAINS-KEY in where clause
ex:    (select :foo (where [[contains :foo  ...]]]))"
  cql/contains-key)

(defn ^:private add-tail
  [x]
  [+ x])

(defn ^:private add-head
  [x]
  [x +])

(defn ^:private remove-tail
  [x]
  [- x])

(defn ^:private remove-head
  [x]
  [x -])

(def inc-by
  "Increment counter by x, usable in `values` and `set-columns`"
  add-tail)

(def dec-by
  "Decrement counter by x, usable in `values` and `set-columns`"
  remove-head)

(def prepend
  "Prepend element to List"
  add-head)

(def append
    "Append/conjoin element to Map/Set/List"
    add-tail)
