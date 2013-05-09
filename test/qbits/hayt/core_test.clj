(ns qbits.hayt.core-test
  (:use clojure.test
        qbits.hayt
        qbits.hayt.codec.joda-time
        [qbits.hayt.cql :only [cql-value cql-identifier]])
  (:import (java.nio ByteBuffer)
           (org.joda.time DateTime)))

(deftest test-select
  (are [expected query] (= expected (->raw query))
       "SELECT * FROM foo;"
       (select :foo)

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

       "SELECT * FROM foo WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
       (select :foo
               (where {:foo :bar
                       :moo [> 3]
                       :meh [:> 4]
                       :baz [:in [5 6 7]]}))

       "SELECT * FROM foo WHERE foo > 1 AND foo < 10;"
       (select :foo
               (where [[:foo  [> 1]]
                       [:foo  [< 10]]])))

  ;;
  (is (= ["SELECT * FROM foo WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          [:bar 3 4 5 6 7]]
         (->prepared (select :foo
                             (where {:foo :bar
                                     :moo [> 3]
                                     :meh [:> 4]
                                     :baz [:in [5 6 7]]}))))))


(deftest test-insert
  (is (= ["INSERT INTO foo (\"c\", a) VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;"
          ["d" "b"]]
         (->prepared (insert :foo
                             (values {"c" "d" :a "b" })
                             (using :timestamp 100000
                                    :ttl 200000)))))

  (is (= "INSERT INTO foo (\"c\", a) VALUES ('d', 'b') USING TIMESTAMP 100000 AND TTL 200000;"
         (->raw (insert :foo
                        (values {"c" "d" :a "b"})
                        (using :timestamp 100000
                               :ttl 200000))))))

(deftest test-update
  (are [expected query] (= expected (->raw query))
       "UPDATE foo SET bar = 1, baz = baz + 2;"
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] }))

       "UPDATE foo SET bar = 1, baz = baz + 2 WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] })
               (where {:foo :bar
                       :moo [> 3]
                       :meh [:> 4]
                       :baz [:in [5 6 7]]}))

       "UPDATE foo SET bar = 1, baz = baz + 2 IF foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] })
               (only-if {:foo :bar
                         :moo [> 3]
                         :meh [:> 4]
                         :baz [:in [5 6 7]]}))

       "UPDATE foo SET bar = 1, baz = baz + 2 IF NOT EXISTS;"
       (update :foo
               (if-not-exists)
               (set-columns {:bar 1
                             :baz [+ 2] }))

       "UPDATE foo SET bar = 1, baz = baz + {'key' : 'value'} WHERE foo = 'bar';"
       (update :foo
               (set-columns {:bar 1
                             :baz [+ {"key" "value"}] })
               (where {:foo :bar}))

       "UPDATE foo SET baz = ['prepended'] + baz WHERE foo = 'bar';"
       (update :foo
               (set-columns {:baz [["prepended"] +] })
               (where {:foo :bar})))


  (are [expected query] (= expected (->prepared query))
       ["UPDATE foo SET bar = ?, baz = baz + ?;" [1 2]]
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] }))

       ["UPDATE foo SET bar = ?, baz = baz + ? WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
        [1 2 :bar 3 4 5 6 7]]
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] })
               (where {:foo :bar
                       :moo [> 3]
                       :meh [:> 4]
                       :baz [:in [5 6 7]]}))))

(deftest test-delete
  (are [expected query] (= expected (->prepared query))
       ["DELETE * FROM foo USING TIMESTAMP 100000 AND TTL 200000 WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
        [:bar 3 4 5 6 7]]
       (delete :foo
               (using :timestamp 100000
                      :ttl 200000)
               (where {:foo :bar
                       :moo [> 3]
                       :meh [:> 4]
                       :baz [:in [5 6 7]]}))

       ["DELETE * FROM foo USING TIMESTAMP 100000 AND TTL 200000 IF foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
        [:bar 3 4 5 6 7]]
       (delete :foo
               (using :timestamp 100000
                      :ttl 200000)
               (only-if {:foo :bar
                         :moo [> 3]
                         :meh [:> 4]
                         :baz [:in [5 6 7]]}))))

(deftest test-ddl
  (are [expected query] (= expected (->raw query))
       "USE foo;"
       (use-keyspace :foo)

       "TRUNCATE foo;"
       (truncate :foo)

       "DROP INDEX foo;"
       (drop-index :foo)

       "DROP KEYSPACE foo;"
       (drop-keyspace :foo)

       "DROP TABLE foo;"
       (drop-table :foo)))

(deftest test-create-index
  (are [expected query] (= expected (->raw query))
       "CREATE INDEX ON foo (bar);"
       (create-index :foo :bar)

       "CREATE INDEX \"baz\" ON foo (bar);"
       (create-index :foo :bar
                     (index-name "baz"))

       "CREATE CUSTOM INDEX ON users (email) WITH options = {'class' : 'path.to.the.IndexClass'};"
       (create-index :users :email
                     (custom true)
                     (with {:options {:class "path.to.the.IndexClass"}}))))

(deftest test-auth-fns
  (are [expected query] (= expected (->raw query))
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
  (are [expected query] (= expected (->raw query))
       "BEGIN BATCH USING TIMESTAMP 2134 \nUPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (\"a\", \"c\") VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;"
       (batch
        (queries
         (update :foo
                 (set-columns {:bar 1
                               :baz [+ 2] }))
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
                               :baz [+ 2] }))
         (insert :foo
                 (values {"a" "b" "c" "d"})
                 (using :timestamp 100000
                        :ttl 200000)))
        (logged false)
        (using :timestamp 2134)))

  (is (= ["BEGIN COUNTER BATCH USING TIMESTAMP 1234 \nUPDATE foo SET bar = ?, baz = baz + ?;\nINSERT INTO foo (\"a\", \"c\") VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;" [1 2 "b" "d"]]
         (->prepared (batch
                      (queries
                       (update :foo
                               (set-columns {:bar 1
                                             :baz [+ 2] }))
                       (insert :foo
                               (values {"a" "b" "c" "d"})
                               (using :timestamp 100000
                                      :ttl 200000)))
                      (counter true)
                      (using :timestamp 1234))))))

(deftest test-create-table
  (are [expected query] (= expected (->raw query))
       "CREATE TABLE foo (a varchar, b int, PRIMARY KEY (a));"
       (create-table :foo
                     (column-definitions {:a :varchar
                                          :b :int
                                          :primary-key :a}))

       "CREATE TABLE foo (foo varchar, bar int, PRIMARY KEY (foo, bar));"
       (create-table :foo
                     (column-definitions {:foo :varchar
                                          :bar :int
                                          :primary-key [:foo :bar]}))

       "CREATE TABLE foo (foo varchar, bar int, PRIMARY KEY (foo, bar)) WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
       (create-table :foo
                     (column-definitions {:foo :varchar
                                          :bar :int
                                          :primary-key [:foo :bar]})
                     (with {:compact-storage true
                            :clustering-order [[:bar :asc]]}))

       "CREATE TABLE foo (foo varchar, bar int, baz text, PRIMARY KEY ((foo, baz), bar)) WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
       (create-table :foo
                     (column-definitions {:foo :varchar
                                          :bar :int
                                          :baz :text
                                          :primary-key [[:foo :baz] :bar]})
                     (with {:compact-storage true
                            :clustering-order [[:bar :asc]]}))))

(deftest test-alter-table
  (are [expected query] (= expected (->raw query))
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

(deftest test-alter-column-family
  (are [expected query] (= expected (->raw query))
       "ALTER COLUMNFAMILY foo ALTER bar TYPE int;"
       (alter-column-family :foo (alter-column :bar :int))

       "ALTER COLUMNFAMILY foo ALTER bar TYPE int ADD baz text RENAME foo TO bar;"
       (alter-column-family :foo
                            (alter-column :bar :int)
                            (rename-column :foo :bar)
                            (add-column :baz :text))

       "ALTER COLUMNFAMILY foo ALTER bar TYPE int ADD baz text WITH CLUSTERING ORDER BY (bar asc) AND COMPACT STORAGE;"
       (alter-column-family :foo
                            (alter-column :bar :int)
                            (add-column :baz :text)
                            (with {:compact-storage true
                                   :clustering-order [[:bar :asc]]}))))

(deftest test-create-alter-keyspace
  (are [expected query] (= expected (->raw query))
       "CREATE KEYSPACE foo WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
       (create-keyspace :foo
                        (with {:replication
                               {:class "SimpleStrategy"
                                :replication_factor 3 }}))

       "CREATE KEYSPACE foo WITH durable_writes = true;"
       (create-keyspace :foo
                        (with {:durable_writes true}))

       "ALTER KEYSPACE foo WITH something-else = 'foo' AND something = 1 AND replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
       (alter-keyspace :foo
                       (with {:replication
                              {:class "SimpleStrategy"
                               :replication_factor 3 }
                              :something 1
                              :something-else "foo"}))))

(deftest test-comp
  (let [q (select :foo)]
    (is (= "SELECT bar, \"baz\" FROM foo;")
        (->raw (merge q (columns :bar "baz"))))

    (is (= ["SELECT bar, \"baz\" FROM foo;" []])
        (->prepared (merge q (columns :bar "baz")))))

  (let [q (insert :foo)
        q2 (merge q (values  {:a "b" "c" "d"}))]
    (is (= "INSERT INTO foo (\"c\", a) VALUES ('d', 'b');"
           (->raw q2)))
    (is (= "INSERT INTO foo (\"c\", a) VALUES ('d', 'b') USING TIMESTAMP 100000 AND TTL 200000;"
           (->raw (merge q2 (using :timestamp 100000
                                   :ttl 200000)))))))


(deftest test-functions
  (are [expected query] (= expected (->raw query))
       "SELECT COUNT(*) FROM foo;"
       (select :foo (columns (count*)))

       "SELECT * FROM foo WHERE ts = now();"
       (select :foo
               (where {:ts (now)}))

       "SELECT WRITETIME(bar) FROM foo;"
       (select :foo (columns (writetime :bar)))

       "SELECT TTL(\"bar\") FROM foo;"
       (select :foo (columns (ttl "bar")))

       "SELECT unixTimestampOf(\"bar\"), dateOf(bar) FROM foo;"
       (select :foo (columns (unix-timestamp-of "bar")
                             (date-of :bar)))

       "SELECT * FROM foo WHERE token(user-id) > token('tom');"
       (select :foo
               (where {(token :user-id) [> (token "tom")]})))

  (are [expected query] (= expected (->prepared query))
       ["SELECT * FROM foo WHERE ts = now();" []]
       (select :foo
               (where {:ts (now)}))

       ["SELECT * FROM foo WHERE token(user-id) > token(?);" ["tom"]]
       (select :foo
               (where {(token :user-id) [> (token "tom")]})))

  (let [d (java.util.Date. 0)]
    (is (= "SELECT * FROM foo WHERE ts > maxTimeuuid(0) AND ts < minTimeuuid(0);"
           (->raw (select :foo
                          (where [[:ts  [> (max-timeuuid d)]]
                                  [:ts  [< (min-timeuuid d)]]])))))))

(deftest test-coll-lookup
  (is (= "DELETE bar[2] FROM foo WHERE baz = 1;"
         (->raw (delete :foo
                        (columns {:bar 2})
                        (where {:baz 1})))))
  (is (= ["DELETE bar[?] FROM foo WHERE baz = ?;" [2 1]]
         (->prepared (delete :foo
                             (columns {:bar 2})
                             (where {:baz 1}))))))

(deftest test-alias
  (are [expected query] (= expected (->raw query))
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
       "a[2]" {:a 2}
       "a['b']" {:a "b"}
       "blobAsBigint(1)" (blob->bigint 1))

  (are [expected value] (= expected (cql-value value))
       "'a'" "a"
       "'a'" :a
       "{'a' : 'b', 'c' : 'd'}" {:a :b :c :d}
       "['a', 'b', 'c', 'd']" ["a" "b" "c" "d"]
       "['a', 'b', 'c', 'd']" '("a" "b" "c" "d")
       "{'a', 'b', 'c', 'd'}" #{"a" "b" "c" "d"}
       1 1))

(deftest test-col-type-sugar
  (are [expected gen] (= expected gen)
    "set<int>" (set-type :int)
    "list<int>" (list-type :int)
    "map<int, text>" (map-type :int :text)))

(deftest test-prepare-map
  (let [query (->prepared (select :foo
                                  (where {:a :a1
                                          :b :b1
                                          :c :c1})))]
    (is (= ["SELECT * FROM foo WHERE a = ? AND c = ? AND b = ?;" [100 300 200]]
           (apply-map query {:a1 100 :b1 200 :c1 300})))))

(deftest test-types
  (let [addr (java.net.InetAddress/getLocalHost)]
    (are [expected query] (= expected (->raw query))
         "SELECT * FROM foo WHERE bar = 0x;"
         (select :foo (where {:bar (ByteBuffer/allocate 0)}))

         "SELECT * FROM foo WHERE bar = 0x62617a;"
         (select :foo (where {:bar (ByteBuffer/wrap (.getBytes "baz"))}))

         "SELECT * FROM foo WHERE bar = 0;"
         (select :foo (where {:bar (java.util.Date. 0)}))

         "SELECT * FROM foo WHERE bar = 0;"
         (select :foo (where {:bar (DateTime. 0)}))


         (str "SELECT * FROM foo WHERE bar = " (.getHostAddress addr) ";")
         (select :foo (where {:bar addr}))

         "SELECT * FROM foo WHERE uuid = 1f84b56b-5481-4ee4-8236-8a3831ee5892;"
         (select :foo (where {:uuid  #uuid "1f84b56b-5481-4ee4-8236-8a3831ee5892"}))

         "INSERT INTO test (v1, c, k) VALUES (null, 1, 0);"
         (insert :test (values {:k 0 :c 1 :v1 nil}))))

  (is (= ["INSERT INTO test (v1, c, k) VALUES (?, ?, ?);" [nil 1 0]]
         (->prepared (insert :test (values {:k 0 :c 1 :v1 nil}))))))
