(ns qbits.hayt.core-test
  (:use clojure.test
        qbits.hayt
        qbits.hayt.cql))

(deftest test-select
  (are [expected query] (= expected (->cql query))
       "SELECT * FROM foo;"
       (select :foo)

       "SELECT bar, baz FROM foo;"
       (select :foo
               (columns :bar "baz"))

       "SELECT bar, baz FROM foo LIMIT 100;"
       (select :foo
               (columns :bar "baz")
               (limit 100))

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
          ["bar" 3 4 5 6 7]]
         (->prepared (select :foo
                             (where {:foo :bar
                                     :moo [> 3]
                                     :meh [:> 4]
                                     :baz [:in [5 6 7]]}))))))




(deftest test-insert
  (is (= ["INSERT INTO foo (a, c) VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;"
          ["b" "d"]]
         (->prepared (insert :foo
                             (values {"a" "b" "c" "d"})
                             (using :timestamp 100000
                                    :ttl 200000)))))

  (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;"
         (->cql (insert :foo
                        (values {"a" "b" "c" "d"})
                        (using :timestamp 100000
                               :ttl 200000))))))

(deftest test-update
  (are [expected query] (= expected (->cql query))
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
        [1 2 "bar" 3 4 5 6 7]]
       (update :foo
               (set-columns {:bar 1
                             :baz [+ 2] })
               (where {:foo :bar
                       :moo [> 3]
                       :meh [:> 4]
                       :baz [:in [5 6 7]]}))))



(deftest test-delete
  (is (= ["DELETE * FROM foo USING TIMESTAMP 100000 AND TTL 200000 WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          ["bar" 3 4 5 6 7]]
         (->prepared (delete :foo
                             (using :timestamp 100000
                                    :ttl 200000)
                             (where {:foo :bar
                                     :moo [> 3]
                                     :meh [:> 4]
                                     :baz [:in [5 6 7]]}))))))

(deftest test-truncate
  (is (= "TRUNCATE foo;"
         (->cql (truncate :foo)))))

(deftest test-drop
  (are [expected query] (= expected (->cql query))
       "DROP INDEX foo;"
       (drop-index :foo)

       "DROP KEYSPACE foo;"
       (drop-keyspace :foo)

       "DROP TABLE foo;"
       (drop-table :foo)))

(deftest test-create-index
  (are [expected query] (= expected (->cql query))
       "CREATE INDEX ON foo (bar);"
       (create-index :foo :bar)

       "CREATE INDEX baz ON foo (bar);"
       (create-index :foo :bar
                     (index-name "baz"))))

(deftest test-batch
  (is (= "BATCH USING TIMESTAMP 2134 \nUPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;"
         (->cql (batch
                 (queries
                  (update :foo
                          (set-columns {:bar 1
                                        :baz [+ 2] }))
                  (insert :foo
                          (values {"a" "b" "c" "d"})
                          (using :timestamp 100000
                                 :ttl 200000)))
                 (using :timestamp 2134)))))

  (is (= ["BATCH USING TIMESTAMP 1234 \nUPDATE foo SET bar = ?, baz = baz + ?;\nINSERT INTO foo (a, c) VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;" [1 2 "b" "d"]]
         (->prepared (batch
                      (queries
                       (update :foo
                               (set-columns {:bar 1
                                             :baz [+ 2] }))
                       (insert :foo
                               (values {"a" "b" "c" "d"})
                               (using :timestamp 100000
                                      :ttl 200000)))
                      (using :timestamp 1234))))))

(deftest test-create-table
  (are [expected query] (= expected (->cql query))
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
                            :clustering-order [[:bar :asc]]}))))

(deftest test-create-alter-keyspace
  (are [expected query] (= expected (->cql query))
       "CREATE KEYPACE foo WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
       (create-keyspace :foo
                        (with {:replication
                               {:class "SimpleStrategy"
                                :replication_factor 3 }}))

       "ALTER KEYPACE foo WITH something-else = 'foo' AND something = 1 AND replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
       (alter-keyspace :foo
                       (with {:replication
                              {:class "SimpleStrategy"
                               :replication_factor 3 }
                              :something 1
                              :something-else "foo"}))))

(deftest test-q->
  (let [q (select :foo)]
    (is (= "SELECT bar, baz FROM foo;")
        (->cql (q-> q
                    (columns :bar "baz"))))

    (is (= ["SELECT bar, baz FROM foo;" []])
        (->prepared (q-> q
                         (columns :bar "baz")))))

  (let [q (insert :foo)
        q2 (q-> q
                (values {"a" "b" "c" "d"}))]
    (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd');"
           (->cql q2)))
    (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;"
           (->cql (q-> q2
                       (using :timestamp 100000
                              :ttl 200000)))))))


(deftest test-functions
  (are [expected query] (= expected (->cql query))
       "SELECT COUNT(*) FROM foo;"
       (select :foo (columns (count*)))

       "SELECT * FROM foo WHERE ts = now();"
       (select :foo
               (where {:ts (now)}))

       "SELECT WRITETIME(bar) FROM foo;"
       (select :foo (columns (writetime "bar")))

       "SELECT TTL(bar) FROM foo;"
       (select :foo (columns (ttl "bar")))

       "SELECT unixTimestampOf(bar), dateOf(bar) FROM foo;"
       (select :foo (columns (unix-timestamp-of "bar")
                             (date-of "bar")))

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
           (->cql (select :foo
                          (where [[:ts  [> (max-timeuuid d)]]
                                  [:ts  [< (min-timeuuid d)]]])))))))

(deftest test-coll-lookup
  (is (= "DELETE bar[2] FROM foo WHERE baz = 1;"
         (->cql (delete :foo
                        (columns {:bar 2})
                        (where {:baz 1})))))
  (is (= ["DELETE bar[?] FROM foo WHERE baz = ?;" [2 1]]
         (->prepared (delete :foo
                             (columns {:bar 2})
                             (where {:baz 1}))))))

(deftest test-cql-identifier
  (are [expected identifier] (= expected (cql-identifier identifier))
       "a" "a"
       "a" :a
       "a[2]" {:a 2}
       "a['b']" {:a "b"}
       "blobAsBigint(1)" (blob->bigint "1"))

  (are [expected value] (= expected (cql-value value))
       "'a'" "a"
       "'a'" :a
       "{'a' : 'b', 'c' : 'd'}" {:a :b :c :d}
       "['a', 'b', 'c', 'd']" ["a" "b" "c" "d"]
       "['a', 'b', 'c', 'd']" '("a" "b" "c" "d")
       "{'a', 'b', 'c', 'd'}" #{"a" "b" "c" "d"}
       1 1))
