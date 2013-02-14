(ns qbits.hayt.core-test
  (:refer-clojure :exclude [set])
  (:use clojure.test
        qbits.hayt
        qbits.hayt.cql))



(deftest test-select

  (is (= ["SELECT * FROM %s;" ["foo"]]
         (as-cql (select :foo))))

  (is (= ["SELECT * FROM %s;" ["foo"]]
         (as-cql (select "foo"))))

  (is (= ["SELECT %s, %s FROM %s;" ["bar" "baz" "foo"]]
         (as-cql (-> (select :foo)
                     (columns :bar "baz")))))

  (is (= ["SELECT %s, %s FROM %s LIMIT 100;" ["bar" "baz" "foo"]]
         (as-cql (-> (select :foo)
                     (columns :bar "baz")
                     (limit 100)))))

  (is (= ["SELECT * FROM %s ORDER BY %s %s;" ["foo" "bar" "desc"]]
         (as-cql (-> (select :foo)
                     (order-by [:bar :desc])))))

  (is (= ["SELECT * FROM %s WHERE %s = %s AND %s > %s AND %s > %s AND %s IN (%s, %s, %s);"
          ["foo" "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-cql (-> (select :foo)
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]}))))))

(deftest test-insert
  (is (= ["INSERT INTO %s (%s, %s) VALUES (%s, %s) USING TIMESTAMP %s AND TTL %s;"
          ["foo" "a" "c" "'b'" "'d'" 100000 200000]]
         (as-cql (-> (insert :foo)
                     (values {"a" "b" "c" "d"})
                     (using :timestamp 100000
                            :ttl 200000))))))

(deftest test-update
  (is (= ["UPDATE %s SET %s = %s, %s = %s  %s;" ["foo" "bar" 1 "baz" "baz" 2]]
         (as-cql (-> (update :foo)
                     (set {:bar 1
                           :baz [:+= 2] })))))

  (is (= ["UPDATE %s SET %s = %s, %s = %s  %s WHERE %s = %s AND %s > %s AND %s > %s AND %s IN (%s, %s, %s);"
          ["foo" "bar" 1 "baz" "baz" 2 "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-cql (-> (update :foo)
                     (set {:bar 1
                           :baz [:+= 2] })
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]}))))))


(deftest test-delete
  (is (= ["DELETE * FROM %s USING TIMESTAMP %s AND TTL %s WHERE %s = %s AND %s > %s AND %s > %s AND %s IN (%s, %s, %s);"
          ["foo" 100000 200000 "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-cql (-> (delete :foo)
                     (using :timestamp 100000
                            :ttl 200000)
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]}))))))

(deftest test-truncate
  (is (= ["TRUNCATE %s;" ["foo"]]
         (as-cql (truncate :foo)))))

(deftest test-drop
  (is (= ["DROP INDEX %s;" ["foo"]]
         (as-cql (drop-index :foo))))

  (is (= ["DROP KEYSPACE %s;" ["foo"]]
         (as-cql (drop-keyspace :foo))))

  (is (= ["DROP TABLE %s;" ["foo"]]
         (as-cql (drop-table :foo)))))

(deftest test-create-index
  (is (= ["CREATE INDEX ON %s ( %s );" ["foo" "bar"]]
         (as-cql (create-index :foo :bar))))

  (is (= ["CREATE INDEX %s ON %s ( %s );" ["baz" "foo" "bar"]]
         (as-cql (-> (create-index :foo :bar)
                     (index-name "baz"))))))
