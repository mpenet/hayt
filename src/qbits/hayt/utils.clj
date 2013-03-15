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

;; Utilities

(defn apply-map
  "Takes a generated prepared query with its arg vector containing
  keywords for placeholders and maps the supplied map to it"
  [[query placeholders] parameter-map]
  [query (replace parameter-map placeholders)])
