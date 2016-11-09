(ns qbits.hayt.spec
  (:require
   [clojure.string :as str]
   [qbits.hayt.cql :as cql]
   [qbits.hayt.utils :as utils]
   [clojure.spec :as s]
   [clojure.test.check.generators :as gen]
   [qbits.spex :as sx]))

;; both should be refined since it's an open protocol
;; todo quoted strings
(s/def ::cql-identifier
  (s/or :string string?
        :keyword keyword?
        :map map?
        :symbol symbol?
        :bytes bytes?))

;; be more precise for table/column names
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

(s/exercise ::cql-type)

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
(defmulti statement cql/find-entry-clause)

(defmethod statement :select
  [query]
  (s/keys :req-un [::select]
          :opt-un [::select ::from ::columns ::where ::order-by ::limit
                   ::only-if]))

(defmethod statement :insert
  [query]
  (s/keys :req-un [::insert ::values]
          :opt-un [::if-exists]))

(defmethod statement :update
  [query]
  (s/keys :req-un [::update ::set-columns ::where]
          :opt-un [::using ::if ::if-exists]))

(defmethod statement :delete
  [query]
  (s/keys :req-un [::update ::set-columns ::where]
          :opt-un [::using ::if ::if-exists]))

(defmethod statement :use-keyspace
  [query]
  (s/keys :req-un [::use-keyspace]))

(defmethod statement :truncate
  [query]
  (s/keys :req-un [::truncate]))

(defmethod statement :drop-index
  [query]
  (s/keys :req-un [::drop-index]
          :opt-un [::if-exists]))

(defmethod statement :drop-type
  [query]
  (s/keys :req-un [::drop-type]
          :opt-un [::if-exists]))

(defmethod statement :drop-table
  [query]
  (s/keys :req-un [::drop-table]
          :opt-un [::if-exists]))

(defmethod statement :drop-keyspace
  [query]
  (s/keys :req-un [::drop-keyspace]
          :opt-un [::if-exists]))

(defmethod statement :drop-column-family
  [query]
  (s/keys :req-un [::drop-column-family]
          :opt-un [::if-exists]))

(defmethod statement :create-index
  [query]
  (s/and (s/keys :req-un [::create-index ::on]
                 :opt-un [::if-exists ::with ::custom])
         #(or (not (contains? % :with))
              (and (contains? % :with) (contains? % :custom)))))

(defmethod statement :create-trigger
  [query]
  (s/keys :req-un [::create-trigger ::on]
          :opt-un [::using]))

(defmethod statement :drop-trigger
  [query]
  (s/keys :req-un [::drop-trigger ::on]))

(defmethod statement :grant
  [query]
  (s/keys :req-un [::grant]
          :opt-un [::perm ::resource ::user]))

(defmethod statement :revoke
  [query]
  (s/keys :req-un [::revoke]
          :opt-un [::perm ::resource ::user]))

(defmethod statement :create-user
  [query]
  (s/keys :req-un [::create-user ::password]
          :opt-un [::superuser ::if-exists]))

(defmethod statement :alter-user
  [query]
  (s/keys :req-un [::alter-user ::password]
          :opt-un [::superuser]))

(defmethod statement :drop-user
  [query]
  (s/keys :req-un [::drop-user]
          :opt-un [::if-exists]))

(defmethod statement :list-users
  [query]
  (s/keys :req-un [::list-users]))

(defmethod statement :list-perm
  [query]
  (s/keys :req-un [::list-perm ::resource ::user]
          :opt-un [::perm ::recursive]))

(defmethod statement :create-table
  [query]
  (s/keys :req-un [::create-table ::column-definitions]
          :opt-un [::if-exists ::with]))

(defmethod statement :alter-table
  [query]
  (s/keys :req-un [::alter-table (or ::add-column ::rename-column ::drop-column
                                     ::alter-column)]
          :opt-un [::with]))

(defmethod statement :alter-columnfamily
  [query]
  (s/keys :req-un [::alter-column-family (or ::add-column ::rename-column
                                             ::drop-column ::alter-column)]
          :opt-un [::with]))

(defmethod statement :alter-keyspace
  [query]
  (s/keys :req-un [::alter-keyspace ::with]))

(defmethod statement :create-type
  [query]
  (s/keys :req-un [::create-type ::column-definitions]
          :opt-un [::if-exists]))

(defmethod statement :alter-type
  [query]
  (s/keys :req-un [::alter-type (or ::add-column ::alter-column ::rename-column
                                    ::drop-column)]
          :opt-un [::with]))

(s/def ::statement (s/multi-spec statement :entry-clause))

;; select
(s/def ::select ::cql-identifier*)
(s/def ::from ::cql-identifier*)
(s/def ::columns ::cql-identifier*)
(s/def ::limit pos-int?)
(s/def ::only-if ::if)
(s/def ::where ::clauses)
(s/def ::order-by (s/cat :column ::cql-identifier* :order #{:desc :asc}))
(s/def ::operator (into #{} (keys cql/operators)))

;; insert
(s/def ::insert ::cql-identifier*)
(s/def ::if-exists boolean?)

(s/def ::values (s/+ (s/spec (s/cat :column ::cql-identifier
                                    :value ::cql-value))))

;; update
(s/def ::update ::cql-identifier*)
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

(sx/ns-as 'qbits.hayt.spec.using 'using)
(s/def ::using/timestamp pos-int?)
(s/def ::using/ttl pos-int?)
(s/def ::using (s/keys :opt-un [::using/ttl ::using/timestamp]))

(s/def ::column-definitions (s/tuple ::cql-identifier*
                                     ::cql-type))


(s/def ::use-keyspace ::cql-identifier*)
(s/def ::truncate ::cql-identifier*)
(s/def ::drop-index ::cql-identifier*)
(s/def ::drop-type ::cql-identifier*)
(s/def ::drop-table ::cql-identifier*)
(s/def ::drop-keyspace ::cql-identifier*)
(s/def ::drop-column-family ::cql-identifier*)
(s/def ::create-index ::cql-identifier*)
(s/def ::create-trigger ::cql-identifier*)
(s/def ::drop-trigger ::cql-identifier*)
(s/def ::grant ::cql-identifier*)
(s/def ::revoke ::cql-identifier*)

(s/def ::create-user ::cql-identifier*)
(s/def ::alter-user ::cql-identifier*)
(s/def ::drop-user ::cql-identifier*)

(s/def ::list-users nil?)
(s/def ::list-perm ::perm)

;; (s/def ::batch ::cql-identifier)

(s/def ::create-table ::cql-identifier*)
(s/def ::alter-table ::cql-identifier*)

(s/def ::add-column (s/cat :column ::cql-identifier
                           :type ::cql-type
                           :static (s/? ::cql-static-type)))
(s/def ::rename-column (s/tuple ::cql-identifier* ::cql-identifier))
(s/def ::alter-column (s/cat :column ::cql-identifier*
                             :type ::cql-type
                             :static (s/? ::cql-static-type)))
(s/def ::drop-column ::cql-identifier*)



(s/def ::alter-column-family ::cql-identifier*)
(s/def ::alter-keyspace ::cql-identifier*)
(s/def ::create-type ::cql-identifier*)
(s/def ::alter-type ::cql-identifier*)
