(ns qbits.hayt.core-test
  (:refer-clojure :exclude [update])
  (:use clojure.test
        qbits.hayt
        qbits.hayt.codec.joda-time
        [qbits.hayt.cql :only [cql-value cql-identifier]])
  (:import (java.nio ByteBuffer)
           (org.joda.time DateTime)))

(defmacro are-raw [& body]
  `(are [expected query] (= expected (->raw query))
        ~@body))

(defmacro are-prepared [& body]
  `(are [expected query] (= expected (->prepared query))
        ~@body))

(def baz-bytes (ByteBuffer/wrap (.getBytes "baz")))
(def baz-blob "0x62617a")

(deftest test-select
  (are-raw
   "SELECT * FROM foo;"
   (select :foo)

   "SELECT * FROM foo.\"bar\";"
   (select (cql-ns :foo "bar"))

   "SELECT DISTINCT bar FROM foo;"
   (select :foo (columns (distinct* :bar)))

   "SELECT DISTINCT \"bar\" FROM foo;"
   (select :foo (columns (distinct* "bar")))

   "SELECT DISTINCT foo, bar, baz FROM foo;"
   (select :foo (columns (distinct* :foo :bar :baz)))

   "SELECT bar, \"baz\" FROM foo;"
   (select :foo
           (columns :bar "baz"))

   "SELECT bar, \"baz\" FROM foo LIMIT 100 ALLOW FILTERING;"
   (select :foo
           (columns :bar "baz")
           (limit 100)
           (allow-filtering true))

   "SELECT * FROM foo ORDER BY bar desc;"
   (select :foo
           (order-by [:bar :desc]))

   "SELECT * FROM foo WHERE foo = 'bar' AND (a, b, \"c\") = ('a', 'b', 'c') AND (a, b, \"d\") > ('a', 'b', 'c') AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7) AND baz CONTAINS 1 AND baz CONTAINS KEY 1;"
   (select :foo
           (where [[= :foo "bar"]
                   [= [:a :b "c"] ["a" "b" "c"]]
                   [> [:a :b "d"] ["a" "b" "c"]]
                   [> :moo 3]
                   [:> :meh 4]
                   [:in :baz [5 6 7]]
                   [contains :baz 1]
                   [contains-key :baz 1]]))

   "SELECT * FROM foo WHERE foo > 1 AND foo < 10;"
   (select :foo
           (where [[> :foo 1]
                   [< :foo 10]]))

   "SELECT * FROM foo WHERE (a, b, \"c\") = ('a', 'b', 'c') AND foo = 1 AND foo2 = 10;"
   (select :foo
           (where {:foo2 10
                   :foo 1
                   [:a :b "c"] ["a" "b" "c"]}))

   "SELECT * FROM foo WHERE foo > 1 AND foo < 10;"
   (select :foo
           (where [[> :foo 1]
                   [< :foo 10]]))

   "SELECT * FROM foo WHERE foo > :param1 AND foo < :param2 AND bar IN :param3 AND baz IN (:param4);"
   (select :foo
           (where' [> :foo :param1]
                   [< :foo :param2]
                   [in :bar :param3]
                   [in :baz [:param4]]))

   "SELECT * FROM foo WHERE foo > :param1 AND foo2 < :param2 AND bar IN :param3 AND baz IN (:param4);"
   (select :foo
           (where1 {:foo [> :param1]
                    :foo2 [< :param2]
                    :bar [:in :param3]
                    :baz [:in [:param4]]}))

   "SELECT * FROM foo WHERE foo > :param1 AND foo2 < :param2 AND bar IN :param3 AND baz IN (:param4);"
   (select :foo
           (where1 :foo [> :param1]
                   :foo2 [< :param2]
                   :bar [:in :param3]
                   :baz [:in [:param4]]))

   "SELECT * FROM foo WHERE foo > :param1 AND foo2 < :param2 AND bar IN :param3 AND baz IN (:param4);"
   (select :foo
           (where1 [[:foo [> :param1]]
                    [:foo2 [< :param2]]
                    [:bar [:in :param3]]
                    [:baz [:in [:param4]]]]))

   "SELECT * FROM foo WHERE foo > ? AND bar IN ? AND foo < 2;"
   (select :foo
           (where [[> :foo ?]
                   [:in :bar ?]
                   [< :foo 2]])))

  (are-prepared
   ["SELECT * FROM foo WHERE foo = ? AND moo > ? AND (a, b, \"d\") > (?, ?, ?) AND meh > ? AND baz IN ?;"
    ["bar" 3 "a" "b" "c" 4 [5 6 7]]]
   (select :foo
           (where [[= :foo "bar"]
                   [> :moo 3]
                   [> [:a :b "d"] ["a" "b" "c"]]
                   [:> :meh 4]
                   [:in :baz [5 6 7]]]))))

(deftest test-insert
  (are-prepared
   ["INSERT INTO foo (\"c\", a) VALUES (?, ?) USING TTL ? AND TIMESTAMP ?;"
    ["d" "b" 200000 100000]]
   (insert :foo
           (values {"c" "d" :a "b" })
           (using {:timestamp 100000
                   :ttl 200000})))

  (are-raw

   "INSERT INTO foo (a, b) VALUES (?, ?);"
   (insert :foo
           (values [[:a ?] [:b ?]]))

   "INSERT INTO foo (\"c\", a) VALUES ('d', 'b') IF NOT EXISTS USING TIMESTAMP 100000 AND TTL 200000;"
   (insert :foo
           (values [["c" "d"] [:a "b"]])
           (if-exists false)
           (using :timestamp 100000
                  :ttl 200000))))

(deftest test-update
  (are-raw
   "UPDATE foo SET bar = 1, baz = baz + 2;"
   (update :foo
           (set-columns :bar 1
                        :baz [+ 2]))

   "UPDATE foo SET bar = 1, baz = baz + 2 WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
   (update :foo
           (set-columns [[:bar 1]
                         [:baz [+ 2]]])
           (where [[:foo "bar"]
                   [> :moo 3]
                   [:> :meh 4]
                   [:in :baz [5 6 7]]]))

   "UPDATE foo SET bar = 1, baz = baz + 2 IF foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
   (update :foo
           (set-columns {:bar 1
                         :baz [+ 2]})
           (only-if [[= :foo "bar"]
                     [> :moo 3]
                     [:> :meh 4]
                     [:in :baz [5 6 7]]]))

   "UPDATE foo SET bar = 1, baz = baz + 2 IF NOT EXISTS;"
   (update :foo
           (if-not-exists)
           (set-columns {:bar 1
                         :baz [+ 2] }))

   "UPDATE foo SET bar = 1, baz = baz + {'key' : 'value'} WHERE foo = 'bar';"
   (update :foo
           (set-columns {:bar 1
                         :baz [+ {"key" "value"}]})
           (where [[:foo "bar"]]))

   "UPDATE foo SET baz = ['prepended'] + baz WHERE foo = 'bar';"
   (update :foo
           (set-columns {:baz (prepend ["prepended"])})
           (where [[:foo "bar"]]))

   "UPDATE foo SET baz = ['prepended'] + baz WHERE foo = 'bar';"
   (update :foo
           (set-columns {:baz [["prepended"] +]})
           (where [[:foo "bar"]])))


  (are-prepared
   ["UPDATE foo SET bar = ?, baz = baz + ?;" [1 2]]
   (update :foo
           (set-columns {:bar 1
                         :baz (inc-by 2)}))


   ["UPDATE foo SET bar = ?, baz = baz + ?;" [1 2]]
   (update :foo
           (set-columns {:bar 1
                         :baz [+ 2]}))


   ["UPDATE foo SET bar = ?, baz = baz + ? WHERE foo = ? AND moo > ? AND meh > ? AND baz IN ?;"
    [1 2 "bar" 3 4 [5 6 7]]]
   (update :foo
           (set-columns {:bar 1
                         :baz [+ 2]})
           (where [[:foo "bar"]
                   [> :moo 3]
                   [:> :meh  4]
                   [:in :baz [5 6 7]]]))))

(deftest test-delete
  (are-prepared
   ["DELETE FROM foo USING TIMESTAMP ? AND TTL ? WHERE foo = ? AND moo > ? AND meh > ? AND baz IN ?;"
    [100000 200000 "bar" 3 4 [5 6 7]]]
   (delete :foo
           (using :timestamp 100000
                  :ttl 200000)
           (where [[:foo "bar"]
                   [> :moo 3]
                   [:> :meh 4]
                   [:in :baz [5 6 7]]]))

   ["DELETE FROM foo USING TIMESTAMP ? AND TTL ? IF foo = ? AND moo > ? AND meh > ? AND baz IN ?;"
    [100000 200000 "bar" 3 4 [5 6 7]]]
   (delete :foo
           (using :timestamp 100000
                  :ttl 200000)
           (only-if [[:foo "bar"]
                     [> :moo 3]
                     [:> :meh 4]
                     [:in :baz [5 6 7]]]))))

(deftest test-ddl
  (are-raw
   "USE foo;"
   (use-keyspace :foo)

   "TRUNCATE foo;"
   (truncate :foo)

   "DROP INDEX foo;"
   (drop-index :foo)

   "DROP INDEX IF EXISTS foo;"
   (drop-index :foo (if-exists))

   "DROP KEYSPACE foo;"
   (drop-keyspace :foo)

   "DROP TABLE foo;"
   (drop-table :foo)))

(deftest test-create-index
  (are-raw
   "CREATE INDEX ON foo (bar);"
   (create-index :foo :bar)

   "CREATE INDEX \"baz\" ON foo (bar);"
   (create-index :foo :bar
                 (index-name "baz"))

   "CREATE CUSTOM INDEX ON users (email) WITH options = {'class' : 'path.to.the.IndexClass'};"
   (create-index :users :email
                 (custom true)
                 (with {:options {:class "path.to.the.IndexClass"}}))))

(deftest test-trigger
  (are-raw
   "CREATE TRIGGER foo ON bar USING 'baz';"
   (create-trigger :foo :bar :baz)

   "DROP TRIGGER foo ON bar;"
   (drop-trigger :foo :bar)))

(deftest test-auth-fns
  (are-raw
   "GRANT PERMISSION FULL_ACCESS ON bar TO baz;"
   (grant :full-access
          (resource :bar)
          (user :baz))

   "REVOKE PERMISSION FULL_ACCESS ON bar FROM baz;"
   (revoke :FULL_ACCESS
           (user :baz)
           (resource :bar))

   "CREATE USER foo WITH PASSWORD bar NOSUPERUSER;"
   (create-user :foo (password :bar))

   "CREATE USER foo WITH PASSWORD bar SUPERUSER;"
   (create-user :foo
                (password :bar)
                (superuser true))

   "ALTER USER foo WITH PASSWORD bar NOSUPERUSER;"
   (alter-user :foo
               (password :bar))

   "ALTER USER foo WITH PASSWORD bar SUPERUSER;"
   (alter-user :foo
               (password :bar)
               (superuser true))

   "DROP USER foo;"
   (drop-user :foo)

   "LIST USERS;"
   (list-users)

   "LIST PERMISSIONS ALL ON bar OF baz;"
   (list-perm (perm :ALL)
              (resource :bar)
              (user :baz))

   "LIST PERMISSIONS ALL ON bar OF baz;"
   (list-perm (resource :bar)
              (user :baz))

   "LIST PERMISSION ALTER ON bar OF baz NORECURSIVE;"
   (list-perm (perm :ALTER)
              (resource :bar)
              (user :baz)
              (recursive false))))


(deftest test-batch
  (are-raw
   "BEGIN BATCH USING TIMESTAMP 2134 \nUPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (\"a\", \"c\") VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;"
   (batch
    (queries
     (update :foo
             (set-columns {:bar 1
                           :baz [+ 2]}))
     (insert :foo
             (values {"a" "b" "c" "d"})
             (using :timestamp 100000
                    :ttl 200000)))
    (using :timestamp 2134))

   "BEGIN UNLOGGED BATCH USING TIMESTAMP 2134 \nUPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (\"a\", \"c\") VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;"
   (batch
    (queries
     (update :foo
             (set-columns {:bar 1
                           :baz [+ 2]}))
     (insert :foo
             (values {"a" "b" "c" "d"})
             (using :timestamp 100000
                    :ttl 200000)))
    (logged false)
    (using :timestamp 2134)))

  (are-prepared
   ["BEGIN COUNTER BATCH USING TIMESTAMP ? \nUPDATE foo SET bar = ?, baz = baz + ?;\nINSERT INTO foo (\"a\", \"c\") VALUES (?, ?) USING TIMESTAMP ? AND TTL ?;\n APPLY BATCH;" [1234 1 2 "b" "d" 100000 200000]]
   (batch
    (queries
     (update :foo
             (set-columns {:bar 1
                           :baz [+ 2]}))
     (insert :foo
             (values {"a" "b" "c" "d"})
             (using :timestamp 100000
                    :ttl 200000)))
    (counter true)
    (using :timestamp 1234))))

(deftest test-create-table
  (are-raw
   "CREATE TABLE IF NOT EXISTS foo (a varchar, b int, c int static, PRIMARY KEY (a));"
   (create-table :foo
                 (if-not-exists)
                 (column-definitions [[:a :varchar]
                                      [:b :int]
                                      [:c :int :static]
                                      [:primary-key :a]]))

   "CREATE TABLE foo (bar int, foo varchar, PRIMARY KEY (foo, bar));"
   (create-table :foo
                 (column-definitions {:foo :varchar
                                      :bar :int
                                      :primary-key [:foo :bar]}))

   "CREATE TABLE foo (bar int, foo varchar, PRIMARY KEY (foo, bar)) WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
   (create-table :foo
                 (column-definitions {:foo :varchar
                                      :bar :int
                                      :primary-key [:foo :bar]})
                 (with {:compact-storage true
                        :clustering-order [[:bar :asc]]}))

   "CREATE TABLE foo (baz text, bar int, foo varchar, PRIMARY KEY ((foo, baz), bar)) WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
   (create-table :foo
                 (column-definitions {:foo :varchar
                                      :bar :int
                                      :baz :text
                                      :primary-key [[:foo :baz] :bar]})
                 (with {:compact-storage true
                        :clustering-order [[:bar :asc]]}))

   "CREATE TABLE foo (a varchar, b list<int>, PRIMARY KEY (ab));"
   (create-table :foo
                 (column-definitions {:a :varchar
                                      :b (list-type :int)
                                      :primary-key :ab}))))

(deftest test-create-type
  (are-raw
   "CREATE TYPE IF NOT EXISTS foo (a varchar, b int, c int static);"
   (create-type :foo
                 (if-not-exists)
                 (column-definitions [[:a :varchar]
                                      [:b :int]
                                      [:c :int :static]]))

   "CREATE TYPE foo (bar int, foo varchar);"
   (create-type :foo
                 (column-definitions {:foo :varchar
                                      :bar :int}))))

(deftest test-alter-table
  (are-raw
   "ALTER TABLE foo ALTER bar TYPE int;"
   (alter-table :foo (alter-column :bar :int))

   "ALTER TABLE foo ALTER bar TYPE int ADD baz text RENAME foo TO bar DROP baz;"
   (alter-table :foo
                (alter-column :bar :int)
                (add-column :baz :text)
                (rename-column :foo :bar)
                (drop-column :baz))

   "ALTER TABLE foo ALTER bar TYPE int ADD baz text WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
   (alter-table :foo
                (alter-column :bar :int)
                (add-column :baz :text)
                (with {:compact-storage true
                       :clustering-order [[:bar :asc]]}))))

(deftest test-alter-columnfamily
  (are-raw
   "ALTER COLUMNFAMILY foo ALTER bar TYPE int;"
   (alter-columnfamily :foo (alter-column :bar :int))

   "ALTER COLUMNFAMILY foo ALTER bar TYPE int ADD baz text RENAME foo TO bar;"
   (alter-columnfamily :foo
                       (alter-column :bar :int)
                       (rename-column :foo :bar)
                       (add-column :baz :text))

   "ALTER COLUMNFAMILY foo ALTER bar TYPE int ADD baz text WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
   (alter-columnfamily :foo
                       (alter-column :bar :int)
                       (add-column :baz :text)
                       (with {:compact-storage true
                              :clustering-order [[:bar :asc]]}))

   "ALTER COLUMNFAMILY foo ALTER bar TYPE int ADD baz text WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
   (alter-column-family :foo
                        (alter-column :bar :int)
                        (add-column :baz :text)
                        (with {:compact-storage true
                               :clustering-order [[:bar :asc]]}))))

(deftest test-create-alter-keyspace
  (are-raw
   "CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
   (create-keyspace :foo
                    (if-exists false)
                    (with {:replication
                           {:class "SimpleStrategy"
                            :replication_factor 3}}))

   "CREATE KEYSPACE foo WITH durable_writes = true;"
   (create-keyspace :foo
                    (with {:durable_writes true}))

   "ALTER KEYSPACE foo WITH something-else = 'foo' AND replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3} AND something = 1;"
   (alter-keyspace :foo
                   (with {:replication
                          {:class "SimpleStrategy"
                           :replication_factor 3}
                          :something 1
                          :something-else "foo"}))))

(deftest test-comp
  (let [q (select :foo)]
    (are-raw
     "SELECT bar, \"baz\" FROM foo;"
     (merge q (columns :bar "baz")))

    (are-prepared
     ["SELECT bar, \"baz\" FROM foo;" []]
     (merge q (columns :bar "baz"))))

  (let [q (insert :foo)
        q2 (merge q (values  {:a "b" "c" "d"}))]
    (are-raw "INSERT INTO foo (\"c\", a) VALUES ('d', 'b');" q2)
    (are-raw
     "INSERT INTO foo (\"c\", a) VALUES ('d', 'b') USING TIMESTAMP 100000 AND TTL 200000;"
     (merge q2 (using :timestamp 100000
                      :ttl 200000)))))


(deftest test-functions
  (are-raw
   "SELECT COUNT(*) FROM foo;"
   (select :foo (columns (count*)))

   "SELECT * FROM foo WHERE ts = now();"
   (select :foo
           (where {:ts (now)}))

   "SELECT WRITETIME(bar) FROM foo;"
   (select :foo (columns (writetime :bar)))

   "SELECT TTL(\"bar\") FROM foo;"
   (select :foo (columns (ttl "bar")))

   (format "SELECT dateOf(blobAsTimeuuid(%s)) FROM foo;" baz-blob)
   (select :foo (columns (date-of (blob->timeuuid baz-bytes))))

   "SELECT unixTimestampOf(0), dateOf(bar) FROM foo;"
   (select :foo (columns (unix-timestamp-of 0)
                         (date-of :bar)))

   "SELECT * FROM foo WHERE token(user-id) > token('tom');"
   (select :foo
           (where [[> (token :user-id) (token "tom")]])))

  "SELECT * FROM foo WHERE token(company-id, user-id) > token('company', 'tom');"
  (select :foo
          (where [[> (token :company-id :user-id) (token "company" "tom")]]))

  (are-prepared
   ["SELECT * FROM foo WHERE ts = now();" []]
   (select :foo
           (where [[= :ts (now)]]))

   ["SELECT * FROM foo WHERE token(user-id) > token(?);" ["tom"]]
   (select :foo
           (where [[> (token :user-id) (token "tom")]])))

  (let [d (java.util.Date. 0)]
    (are-raw
     "SELECT * FROM foo WHERE ts > maxTimeuuid(0) AND ts < minTimeuuid(0);"
     (select :foo
             (where [[> :ts  (max-timeuuid d)]
                     [< :ts  (min-timeuuid d)]])))))

(deftest test-coll-lookup
  (are-raw
   "DELETE bar[2] FROM foo WHERE baz = 1;"
   (delete :foo
           (columns {:bar 2})
           (where [[= :baz 1]])))

  (are-prepared
   ["DELETE bar[?] FROM foo WHERE baz = ?;" [2 1]]
   (delete :foo
           (columns {:bar 2})
           (where [[= :baz 1]]))))

(deftest test-alias
  (are-raw
   "SELECT name AS user_name, occupation AS user_occupation FROM users;"
   (select :users
           (columns (as :name :user_name)
                    (as :occupation :user_occupation)))
   "SELECT COUNT(*) AS user_count FROM users;"
   (select :users
           (columns (as (count*)
                        :user_count)))))


(deftest test-cql-identifier
  (are [expected identifier] (= expected (cql-identifier identifier))
       "\"a\"" "a"
       "a" :a
       "a" 'a
       "a[2]" {:a 2}
       "a['b']" {:a "b"}
       "blobAsBigint(1)" (blob->bigint 1))

  (are [expected value] (= expected (cql-value value))
       "'a'" "a"
       "a" 'a
       ":a" :a
       "{'a' : 'b', 'c' : 'd'}" {"a" "b" "c" "d"}
       "['a', 'b', 'c', 'd']" ["a" "b" "c" "d"]
       "['a', 'b', 'c', 'd']" '("a" "b" "c" "d")
       "{'a', 'b', 'c', 'd'}" (sorted-set "a" "b" "c" "d") ;; #{"a" "b" "c" "d"}
       1 1))

(deftest test-col-type-sugar
  (are [expected gen] (= (keyword expected) gen)
       "set<int>" (set-type :int)
       "list<int>" (list-type :int)
       "map<int, text>" (map-type :int :text)
       "tuple<int, text, int>" (tuple-type :int :text :int)
       ))

(deftest test-types
  (let [addr (java.net.InetAddress/getLocalHost)]
    (are-raw
     "SELECT * FROM foo WHERE bar = 0x;"
     (select :foo (where {:bar (ByteBuffer/allocate 0)}))

     (format "SELECT * FROM foo WHERE bar = %s;" baz-blob)
     (select :foo (where {:bar baz-bytes}))

     (format "SELECT * FROM foo WHERE bar = %s;" baz-blob)
     (select :foo (where {:bar baz-bytes}))

     "SELECT * FROM foo WHERE bar = 0;"
     (select :foo (where {:bar (java.util.Date. 0)}))

     "SELECT * FROM foo WHERE bar = 0;"
     (select :foo (where {:bar (DateTime. 0)}))


     (str "SELECT * FROM foo WHERE bar = " (.getHostAddress addr) ";")
     (select :foo (where {:bar addr}))

     "SELECT * FROM foo WHERE uuid = 1f84b56b-5481-4ee4-8236-8a3831ee5892;"
     (select :foo (where {:uuid  #uuid "1f84b56b-5481-4ee4-8236-8a3831ee5892"}))

     "INSERT INTO test (v1, c, k) VALUES (null, 1, 0);"
     (insert :test (values [[:v1 nil] [:c 1] [:k 0]]))

     "UPDATE user_profiles SET addresses = addresses + {'work' : {city : 'Santa Clara', street : '3975 Freedom Circle Blvd', zip : 95050}} WHERE login = 'tsmith';"
     (update :user_profiles
             (set-columns {:addresses
                           [+ {"work" (user-type
                                       {:street "3975 Freedom Circle Blvd"
                                        :city "Santa Clara"
                                        :zip 95050})}]})
             (where [[= :login "tsmith"]]))

     "INSERT INTO user_profiles (login, first_name, last_name, email, addresses) VALUES ('tsmith', 'Tom', 'Smith', 'tsmith@gmail.com', {'home' : {city : 'San Fransisco', street : '1021 West 4th St. #202', zip : 94110}});"
     (insert :user_profiles (values [[:login "tsmith"]
                                     [:first_name "Tom"]
                                     [:last_name "Smith"]
                                     [:email "tsmith@gmail.com"]
                                     [:addresses {"home" (user-type
                                                          {:street "1021 West 4th St. #202"
                                                           :city "San Fransisco"
                                                           :zip 94110})}]]))))

  (are-prepared
   ["INSERT INTO test (v1, c, k) VALUES (?, ?, ?);" [nil 1 0]]
   (insert :test (values [[:v1 nil] [:c 1] [:k 0]]))))
