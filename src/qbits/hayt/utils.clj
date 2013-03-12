(ns qbits.hayt.utils
  (:require [qbits.hayt.cql :as cql]))

;; Sugar for collection types

(defn coll-type
  "Helps with the generation of Collection types definitions.
Takes a CQL type as keyword and it's arguments: ex (coll-type :map :int :uuid).
The possible collection types are :map, :list and :set."
  [t & spec]
  (format "%s<%s>"
          (name t)
          (cql/join-comma (map name spec))))

(def
  ^{:doc "Generates a map type definition, takes 2 arguments, for
  key and value types"}
  map-type
  (partial coll-type :map))

(def
  ^{:doc "Generates a list type definition, takes a single argument
  indicating the list elements type"}
  list-type
  (partial coll-type :list))

(def
  ^{:doc "Generates a set type definition, takes a single argument
  indicating the set elements type"}
  set-type
  (partial coll-type :set))

;; Utilities

(defn apply-map
  "Takes a generated prepared query with its arg vector containing
  keywords for placeholders and maps the supplied map to it"
  [[query placeholders] parameter-map]
  [query (replace parameter-map placeholders)])
