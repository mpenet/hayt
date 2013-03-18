(ns qbits.hayt.types
  (:require
   [clojure.core.typed :as t])
  (:import
   [clojure.lang
    Named
    APersistentMap
    Seqable]))

;; in case we now type per position, it will enforce it
(t/def-alias SeqPair (TFn [[x :variance :covariant]
                           [y :variance :covariant]]
                          (I (Seqable (U x y))
                             (ExactCount 2))))

;; will be used as a placeholder until replaced
(t/def-alias XMap (APersistentMap Any Any))

(t/def-alias HaytQuery XMap)
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

(t/def-alias CQLIdentifier (U Named String))
(t/def-alias CQLValue Any)

;; clauses
(t/def-alias ColumnsClause '{:columns (Seqable CQLIdentifier)})
(t/def-alias ColumnDefinitionsClause '{:column-definitions
                                       (APersistentMap CQLIdentifier
                                                       (U C*Type '[CQLIdentifier]))})
(t/def-alias UsingClause '{:using (Seqable (U CQLIdentifier Number))})
(t/def-alias LimitClause '{:limit Number})
(t/def-alias OrderByClause '{:order-by (Seqable '[CQLIdentifier (U ':asc ':desc)])})
(t/def-alias QueriesClause '{:queries (Seqable HaytQuery)})
(t/def-alias WhereClause '{:where (APersistentMap CQLIdentifier CQLValue)})
(t/def-alias ValuesClause '{:values (APersistentMap CQLIdentifier CQLValue)})
(t/def-alias SetColumnsClause '{:set-columns (APersistentMap CQLIdentifier CQLValue)})
(t/def-alias WithClause '{:with XMap})
(t/def-alias IndexNameClause '{:index-name CQLIdentifier})
(t/def-alias AlterColumnClause '{:alter-column '[CQLIdentifier C*Type]})
(t/def-alias RenameColumnClause '{:rename-column '[CQLIdentifier CQLIdentifier]})
(t/def-alias AddColumnClause '{:add-column '[CQLIdentifier C*Type]})
(t/def-alias AllowFilteringClause '{:allow-filtering Boolean})
(t/def-alias LoggedClause '{:logged Boolean})
(t/def-alias CounterClause '{:counter Boolean})
(t/def-alias SuperUserClause '{:superuser Boolean})
(t/def-alias PasswordClause '{:password Boolean})
(t/def-alias RecursiveClause '{:recursive Boolean})
(t/def-alias ResourceClause '{:resource CQLIdentifier})
(t/def-alias UserClause '{:user CQLIdentifier})
(t/def-alias PermClause '{:perm (Seqable CQLPermission)})

(t/def-alias AnyOperatorFn [Number * -> (U Boolean Number)]) ;; hairy
(t/def-alias Operator (U ':= ':> ':< ':<= ':=> ':+ ':- AnyOperatorFn
                         ;; '= '> '< '<= '=> '+ '-
                         ))

;; (t/def-alias HaytClause XMap) ;; to be replaced by an union of *Clause
(t/def-alias HaytClause (U ColumnsClause
                           ColumnDefinitionsClause
                           UsingClause
                           LimitClause
                           OrderByClause
                           QueriesClause
                           WhereClause
                           ValuesClause
                           SetColumnsClause
                           WithClause
                           IndexNameClause
                           AlterColumnClause
                           RenameColumnClause
                           AddColumnClause
                           AllowFilteringClause
                           LoggedClause
                           CounterClause
                           SuperUserClause
                           PasswordClause
                           RecursiveClause
                           UserClause
                           PermClause))
;; (t/ann b [(U '{:a '1} '{:b '2} AnyOperatorFn) -> (Value 1)])
;; (defn b [x] 1)

;; 0(t/cf (b +))

;; (def x {:a 1 :b 2})
;; (t/ann x (I Number Integer))
;; (defn b {:columns [:a 1]})

;; (prn (t/cf (a)))


;; could make sense to define a type for possible parameterized values
;; allowing to type check qbits.hayt.cql/maybe-parameterize! for instance
