(ns qbits.hayt.utils
  (:require
   [qbits.hayt.cql :as cql]
   [clojure.core.typed :as t])
  (:import
   [clojure.lang
    Keyword
    APersistentMap]))

(t/def-alias C*CollKeyword (U (Value :list)
                              (Value :map)
                              (Value :set)))

(t/def-alias C*Types (U (Value :ascii)
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

;; Sugar for collection types
(t/ann coll-type (Fn [C*CollKeyword C*Types -> String]
                     [C*CollKeyword C*Types C*Types -> String]))
(defn coll-type
  "Helps with the generation of Collection types definitions.
Takes a CQL type as keyword and it's arguments: ex (coll-type :map :int :uuid).
The possible collection types are :map, :list and :set."
  [t & spec]
  (format "%s<%s>"
          (name t)
          (cql/join-comma (map name spec))))

(t/ann map-type [C*Types C*Types -> String])
(def map-type
  "Generates a map type definition, takes 2 arguments, for key and
  value types"
  (partial coll-type :map))

(t/ann map-type [C*Types -> String])
(def list-type
  "Generates a list type definition, takes a single argument
  indicating the list elements type"
  (partial coll-type :list))

(t/ann set-type [C*Types -> String])
(def set-type
  "Generates a set type definition, takes a single argument indicating
  the set elements type"
  (partial coll-type :set))

;; ;; Utilities

(t/ann apply-map ['[String (Vector* Any)] (APersistentMap Any Any) -> String])
(defn apply-map
  "Takes a generated prepared query with its arg vector containing
  keywords for placeholders and maps the supplied map to it"
  [[query placeholders] parameter-map]
  [query (replace parameter-map placeholders)])
