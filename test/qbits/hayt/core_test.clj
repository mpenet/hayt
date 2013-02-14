(ns qbits.hayt.core-test
  (:refer-clojure :exclude [set])
  (:use clojure.test
        qbits.hayt
        qbits.hayt.cql))

(deftest test-select

  ;;
  (is (= ["SELECT * FROM ?;" ["foo"]]
         (as-prepared (select :foo))))

  (is (= "SELECT * FROM foo;"
         (as-cql (select :foo))))

  ;;
  (is (= ["SELECT ?, ? FROM ?;" ["bar" "baz" "foo"]]
         (as-prepared (-> (select :foo)
                          (columns :bar "baz")))))

  (is (= "SELECT bar, baz FROM foo;"
         (as-cql (-> (select :foo)
                     (columns :bar "baz")))))

  ;;
  (is (= ["SELECT ?, ? FROM ? LIMIT 100;" ["bar" "baz" "foo"]]
         (as-prepared (-> (select :foo)
                          (columns :bar "baz")
                          (limit 100)))))

  (is (= "SELECT bar, baz FROM foo LIMIT 100;"
         (as-cql (-> (select :foo)
                     (columns :bar "baz")
                     (limit 100)))))

  ;;
  (is (= ["SELECT * FROM ? ORDER BY ? ?;" ["foo" "bar" "desc"]]
         (as-prepared (-> (select :foo)
                          (order-by [:bar :desc])))))

  (is (= "SELECT * FROM foo ORDER BY bar desc;"
         (as-cql (-> (select :foo)
                     (order-by [:bar :desc])))))

  ;;
  (is (= ["SELECT * FROM ? WHERE ? = ? AND ? > ? AND ? > ? AND ? IN (?, ?, ?);"
          ["foo" "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-prepared (-> (select :foo)
                          (where {:foo :bar
                                  :moo [> 3]
                                  :meh [:> 4]
                                  :baz [:in [5 6 7]]})))))

  (is (= "SELECT * FROM foo WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (as-cql (-> (select :foo)
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]}))))))

(deftest test-insert
  (is (= ["INSERT INTO ? (?, ?) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?;"
          ["foo" "a" "c" "b" "d" 100000 200000]]
         (as-prepared (-> (insert :foo)
                          (values {"a" "b" "c" "d"})
                          (using :timestamp 100000
                                 :ttl 200000)))))

  (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;"
         (as-cql (-> (insert :foo)
                     (values {"a" "b" "c" "d"})
                     (using :timestamp 100000
                            :ttl 200000))))))

(deftest test-update
  ;;
  (is (= ["UPDATE ? SET ? = ?, ? = ? + ?;" ["foo" "bar" 1 "baz" "baz" 2]]
         (as-prepared (-> (update :foo)
                          (set {:bar 1
                                :baz [+ 2] })))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2;"
         (as-cql (-> (update :foo)
                     (set {:bar 1
                           :baz [+ 2] })))))

  ;;
  (is (= ["UPDATE ? SET ? = ?, ? = ? + ? WHERE ? = ? AND ? > ? AND ? > ? AND ? IN (?, ?, ?);"
          ["foo" "bar" 1 "baz" "baz" 2 "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-prepared (-> (update :foo)
                          (set {:bar 1
                                :baz [+ 2] })
                          (where {:foo :bar
                                  :moo [> 3]
                                  :meh [:> 4]
                                  :baz [:in [5 6 7]]})))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2 WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (as-cql (-> (update :foo)
                     (set {:bar 1
                           :baz [+ 2] })
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]}))))))


(deftest test-delete
  (is (= ["DELETE * FROM ? USING TIMESTAMP ? AND TTL ? WHERE ? = ? AND ? > ? AND ? > ? AND ? IN (?, ?, ?);"
          ["foo" 100000 200000 "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-prepared (-> (delete :foo)
                          (using :timestamp 100000
                                 :ttl 200000)
                          (where {:foo :bar
                                  :moo [> 3]
                                  :meh [:> 4]
                                  :baz [:in [5 6 7]]}))))))

(deftest test-truncate
  (is (= ["TRUNCATE ?;" ["foo"]]
         (as-prepared (truncate :foo)))))

(deftest test-drop
  (is (= ["DROP INDEX ?;" ["foo"]]
         (as-prepared (drop-index :foo))))

  (is (= ["DROP KEYSPACE ?;" ["foo"]]
         (as-prepared (drop-keyspace :foo))))

  (is (= ["DROP TABLE ?;" ["foo"]]
         (as-prepared (drop-table :foo)))))

(deftest test-create-index
  (is (= ["CREATE INDEX ON ? ( ? );" ["foo" "bar"]]
         (as-prepared (create-index :foo :bar))))

  (is (= ["CREATE INDEX ? ON ? ( ? );" ["baz" "foo" "bar"]]
         (as-prepared (-> (create-index :foo :bar)
                          (index-name "baz"))))))


(deftest test-batch
  (is (= "BATCH USING TIMESTAMP 2134 \n UPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000; \nAPPLY BATCH;"
         (as-cql (-> (batch
                      (-> (update :foo)
                          (set {:bar 1
                                :baz [+ 2] }))
                      (-> (insert :foo)
                          (values {"a" "b" "c" "d"})
                          (using :timestamp 100000
                                 :ttl 200000)))
                     (using :timestamp 2134)))))

  (is (= ["BATCH USING TIMESTAMP ? \n UPDATE ? SET ? = ?, ? = ? + ?;\nINSERT INTO ? (?, ?) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?; \nAPPLY BATCH;" [1234 "foo" "bar" 1 "baz" "baz" 2 "foo" "a" "c" "b" "d" 100000 200000]]
         (as-prepared (-> (batch
                           (-> (update :foo)
                               (set {:bar 1
                                     :baz [+ 2] }))
                           (-> (insert :foo)
                               (values {"a" "b" "c" "d"})
                               (using :timestamp 100000
                                      :ttl 200000)))
                          (using :timestamp 1234))))))
