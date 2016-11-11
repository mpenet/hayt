(ns qbits.hayt.spec
  (:require
   [clojure.string :as str]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.utils :as utils]
   [clojure.spec :as s]
   [clojure.test.check.generators :as gen]
   [qbits.spex :as sx]))

;; TODO
;; create separate specs for keyspace/table/column ids so that we have
;; custom gen available the road and allow execution against a real c*
;; instance of valid queries for generated cassandra schemas.

;; both should be refined since it's an open protocol
;; todo quoted strings
(s/def ::cql-identifier
  (s/or :string string?
        :keyword keyword?
        :map map?
        :symbol symbol?
        :bytes bytes?))

;; be more e for table/column names
(s/def ::cql-identifier*
  (s/or :string (s/spec string?
                        :gen (fn []
                               (gen/not-empty gen/string-alphanumeric)))
        :keyword (s/spec keyword?
                         :gen (fn []
                                (gen/fmap keyword
                                          (gen/not-empty gen/string-alphanumeric))))))

;; todo quoted strings
(s/def ::cql-value
  (s/or :string string?
        :number number?
        :keyword keyword?
        :map map?
        :set set?
        :seq seq?
        :coll coll?
        :symbol symbol?
        :bytes bytes?
        :date inst?
        :uuid uuid?
        :nil nil?))

(s/def ::cql-type
  (s/spec #(contains? (set utils/native-types)
                 (keyword %))
          :gen (fn []
                 (gen/let [type (gen/elements utils/native-types)
                           f (gen/elements [name identity])]
                   (f type)))))


(s/def ::cql-static-type #{:static "static"})


(s/def ::clauses
  (s/cat :clause
         (s/spec
          (s/or
           :with-operator
           (s/cat :operator ::operator
                  :column ::cql-identifier
                  :value ::cql-value)
           :without-operator
           (s/cat :column ::cql-identifier
                  :value ::cql-value)))))

(s/def ::if ::clauses)

;; statements
;; (def statement nil)
(defmulti statement cql/find-entry-clause)

(defmethod statement :select
  [query]
  (s/merge (s/keys :req-un [::select])
         ::select-clauses))

(defmethod statement :insert
  [query]
  (s/merge (s/keys :req-un [::insert])
         ::insert-clauses))

(defmethod statement :update
  [query]
  (s/merge (s/keys :req-un [::update])
         ::update-clauses))

(defmethod statement :delete
  [query]
  (s/merge (s/keys :req-un [::delete])
         ::delete-clauses))

(defmethod statement :use-keyspace
  [query]
  (s/keys :req-un [::use-keyspace]))

(defmethod statement :truncate
  [query]
  (s/keys :req-un [::truncate]))

(defmethod statement :drop-index
  [query]

  (s/merge (s/keys :req-un [::drop-index])
         ::drop-index-clauses))

(defmethod statement :drop-type
  [query]

  (s/merge (s/keys :req-un [::drop-type])
         ::drop-type-clauses))

(defmethod statement :drop-table
  [query]
  (s/merge (s/keys :req-un [::drop-table])
         ::drop-table-clauses))

(defmethod statement :drop-keyspace
  [query]
  (s/merge (s/keys :req-un [::drop-keyspace])
         ::drop-keyspace-clauses))

(defmethod statement :drop-column-family
  [query]
  (s/merge (s/keys :req-un [::drop-column-family])
         ::drop-column-family-clauses))

(defmethod statement :create-index
  [query]
  (s/merge (s/keys :req-un [::create-index])
         ::create-index-clauses))

(defmethod statement :create-trigger
  [query]
  (s/merge (s/keys :req-un [::create-trigger])
         ::create-trigger-clauses))

(defmethod statement :drop-trigger
  [query]
  (s/merge (s/keys :req-un [::drop-trigger])
         ::drop-trigger-clauses))

(defmethod statement :grant
  [query]
  (s/merge (s/keys :req-un [::grant])
         ::grant-clauses))

(defmethod statement :revoke
  [query]
  (s/merge (s/keys :req-un [::revoke])
         ::revoke-clauses))

(defmethod statement :create-user
  [query]
  (s/merge (s/keys :req-un [::create-user])
         ::create-user-clauses))

(defmethod statement :alter-user
  [query]
  (s/merge (s/keys :req-un [::alter-user])
         ::alter-user-clauses))

(defmethod statement :drop-user
  [query]
  (s/merge (s/keys :req-un [::drop-user])
         ::drop-user-clauses))

(defmethod statement :list-users
  [query]
  (s/keys :req-un [::list-users]))

(defmethod statement :list-perm
  [query]
  (s/merge (s/keys :req-un [::list-perm])
         ::list-perm-clauses))

(defmethod statement :create-table
  [query]
  (s/merge (s/keys :req-un [::create-table])
         ::create-table-clauses))

(defmethod statement :alter-table
  [query]
  (s/merge (s/keys :req-un [::alter-table])
         ::alter-table-clauses))

(defmethod statement :alter-columnfamily
  [query]
  (s/merge (s/keys :req-un [::alter-column-family])
         ::alter-column-family-clauses))

(defmethod statement :alter-keyspace
  [query]
  (s/merge (s/keys :req-un [::alter-keyspace])
         ::alter-keyspace-clauses))

(defmethod statement :create-type
  [query]
  (s/merge (s/keys :req-un [::create-type])
         ::create-type-clauses))

(defmethod statement :alter-type
  [query]
  (s/merge (s/keys :req-un [::alter-type])
         ::alter-type-clauses))

(s/def ::statement (s/multi-spec statement :entry-clause))

;; select
(s/def ::select ::cql-identifier*)
(s/def ::select-clauses
  (s/keys :opt-un [::from ::columns ::where ::order-by ::limit
                   ::only-if]))

(s/def ::from ::cql-identifier*)
(s/def ::columns ::cql-identifier*)
(s/def ::limit pos-int?)
(s/def ::only-if ::if)
(s/def ::where ::clauses)
(s/def ::order-by (s/cat :column ::cql-identifier* :order #{:desc :asc}))
(s/def ::operator (into #{} (keys cql/operators)))

;; insert
(s/def ::insert ::cql-identifier*)
(s/def ::insert-clauses
  (s/keys :req-un [::values]
          :opt-un [::if-exists]))

(s/def ::if-exists boolean?)

(s/def ::values (s/+ (s/spec (s/cat :column ::cql-identifier
                                    :value ::cql-value))))

;; update
(s/def ::update ::cql-identifier*)
(s/def ::update-clauses
  (s/keys :req-un [::set-columns ::where]
          :opt-un [::using ::if ::if-exists]))

(s/def ::set-column-op #{- + :- :+})
(s/def ::set-columns
  (s/+ (s/spec
        (s/cat :column ::cql-identifier
               :value (s/or :infix-op (s/cat :op ::set-column-op
                                             :val ::cql-value)
                            :postfix-op (s/cat :val ::cql-value
                                               :op ::set-column-op)
                            :assignement ::cql-value)))))

(s/def ::recursive boolean?)

(s/def ::on ::cql-identifier*)
(s/def ::resource ::on)

(s/def ::perm #{:create :alter :drop :select :modify :authorize :describe :execute})

(s/def ::user string?)

(s/def ::custom boolean?)
(s/def ::superuser boolean?)

;; refine
(s/def ::with (s/spec (s/every-kv #(or (keyword? %)
                                       (string? %))
                                  any?)
                      :gen (fn []
                             (gen/one-of
                              [(gen/map (gen/one-of [gen/keyword
                                                     gen/string])
                                        gen/any)
                               (gen/vector (gen/tuple (gen/one-of [gen/keyword
                                                                   gen/string])
                                                      gen/any))]))))
(s/def ::password string?)

(s/def ::delete ::cql-identifier*)
(s/def ::delete-clauses
  (s/keys :req-un [::where]
          :opt-un [::columns ::using ::only-if]))

(sx/ns-as 'qbits.hayt.spec.using 'using)
(s/def ::using/timestamp pos-int?)
(s/def ::using/ttl pos-int?)
(s/def ::using (s/keys :opt-un [::using/ttl ::using/timestamp]))

(s/def ::column-definitions (s/tuple ::cql-identifier*
                                     ::cql-type))


(s/def ::use-keyspace ::cql-identifier*)

(s/def ::truncate ::cql-identifier*)

(s/def ::drop-index ::cql-identifier*)
(s/def ::drop-index-clauses (s/keys :opt-un [::if-exists]))

(s/def ::drop-type ::cql-identifier*)
(s/def ::drop-type-clauses (s/keys :opt-un [::if-exists]))


(s/def ::drop-table ::cql-identifier*)
(s/def ::drop-table-clauses (s/keys :opt-un [::if-exists]))

(s/def ::drop-keyspace ::cql-identifier*)
(s/def ::drop-keyspace-clauses (s/keys :opt-un [::if-exists]))

(s/def ::drop-column-family ::cql-identifier*)
(s/def ::drop-column-family-clauses (s/keys :opt-un [::if-exists]))

(s/def ::create-index ::cql-identifier*)
(s/def ::create-index-clauses
  (s/and (s/keys :req-un [::on]
                 :opt-un [::if-exists ::with ::custom])
         #(or (not (contains? % :with))
              (and (contains? % :with) (contains? % :custom)))))

(s/def ::create-trigger ::cql-identifier*)
(s/def ::create-trigger-clauses
  (s/keys :req-un [::on]
          :opt-un [::using]))

(s/def ::drop-trigger ::cql-identifier*)
(s/def ::drop-trigger-clauses
    (s/keys :req-un [::on]))

(s/def ::grant ::cql-identifier*)
(s/def ::grant-clauses (s/keys :opt-un [::perm ::resource ::user]))

(s/def ::revoke ::cql-identifier*)
(s/def ::revoke-clauses (s/keys :opt-un [::perm ::resource ::user]))

(s/def ::create-user ::cql-identifier*)
(s/def ::create-user-clauses
  (s/keys :req-un [::password]
          :opt-un [::superuser ::if-exists]))


(s/def ::alter-user ::cql-identifier*)
(s/def ::alter-user-clauses
  (s/keys :req-un [::password]
          :opt-un [::superuser]))

(s/def ::drop-user ::cql-identifier*)
(s/def ::drop-user-clauses (s/keys :opt-un [::if-exists]))

(s/def ::list-users nil?)

(s/def ::list-perm ::perm)
(s/def ::list-perm-clauses
  (s/keys :req-un [::resource ::user]
          :opt-un [::perm ::recursive]))

;; (s/def ::batch ::cql-identifier)

(s/def ::create-table ::cql-identifier*)
(s/def ::create-table-clauses
  (s/keys :req-un [::column-definitions]
          :opt-un [::if-exists ::with]))

(s/def ::alter-table ::cql-identifier*)
(s/def ::alter-table-clauses
  (s/keys :req-un [(or ::add-column ::rename-column ::drop-column
                       ::alter-column)]
          :opt-un [::with]))


(s/def ::add-column (s/cat :column ::cql-identifier
                           :type ::cql-type
                           :static (s/? ::cql-static-type)))

(s/def ::rename-column (s/tuple ::cql-identifier* ::cql-identifier))

(s/def ::alter-column (s/cat :column ::cql-identifier*
                             :type ::cql-type
                             :static (s/? ::cql-static-type)))
(s/def ::drop-column ::cql-identifier*)



(s/def ::alter-column-family ::cql-identifier*)
(s/def ::alter-column-family-clauses
  (s/keys :req-un [(or ::add-column ::rename-column
                       ::drop-column ::alter-column)]
          :opt-un [::with]))

(s/def ::alter-keyspace ::cql-identifier*)
(s/def ::alter-keyspace-clauses
  (s/keys :req-un [::with]))

(s/def ::create-type ::cql-identifier*)
(s/def ::create-type-clauses
  (s/keys :req-un [::column-definitions]
          :opt-un [::if-exists]))

(s/def ::alter-type ::cql-identifier*)
(s/def ::alter-type-clauses
  (s/keys :req-un [(or ::add-column ::alter-column ::rename-column
                       ::drop-column)]
          :opt-un [::with]))


;; (s/exercise ::statement )


;; DSL SPECS




;; STATEMENT fns



;; (s/fdef qbits.hayt/raw
;;         :args ::statement
;;         :ret string?)
