(ns qbits.hayt.core-test
  (:use clojure.test
        qbits.hayt
        qbits.hayt.cql))

(deftest test-select

  (is (= "SELECT * FROM foo;"
         (as-cql (select :foo))))


  (is (= "SELECT bar, baz FROM foo;"
         (as-cql (select :foo
                   (columns :bar "baz")))))

  (is (= "SELECT bar, baz FROM foo LIMIT 100;"
         (as-cql (select :foo
                   (columns :bar "baz")
                   (limit 100)))))

  (is (= "SELECT * FROM foo ORDER BY bar desc;"
         (as-cql (select :foo
                   (order-by [:bar :desc])))))

  ;;
  (is (= ["SELECT * FROM foo WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          ["bar" 3 4 5 6 7]]
         (as-prepared (select :foo
                        (where {:foo :bar
                                :moo [> 3]
                                :meh [:> 4]
                                :baz [:in [5 6 7]]})))))

  (is (= "SELECT * FROM foo WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (as-cql (select :foo
                   (where {:foo :bar
                           :moo [> 3]
                           :meh [:> 4]
                           :baz [:in [5 6 7]]}))))))

(deftest test-insert
  (is (= ["INSERT INTO foo (a, c) VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;"
          ["b" "d"]]
         (as-prepared (insert :foo
                        (values {"a" "b" "c" "d"})
                        (using :timestamp 100000
                               :ttl 200000)))))

  (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;"
         (as-cql (insert :foo
                   (values {"a" "b" "c" "d"})
                   (using :timestamp 100000
                          :ttl 200000))))))

(deftest test-update
  ;;
  (is (= ["UPDATE foo SET bar = ?, baz = baz + ?;" [1 2]]
         (as-prepared (update :foo
                        (set-fields {:bar 1
                                     :baz [+ 2] })))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2;"
         (as-cql (update :foo
                   (set-fields {:bar 1
                                :baz [+ 2] })))))

  (prn (as-prepared (update :foo
                      (set-fields {:bar 1
                                   :baz [+ 2] })
                      (where {:foo :bar
                              :moo [> 3]
                              :meh [:> 4]
                              :baz [:in [5 6 7]]}))))

  ;;
  (is (= ["UPDATE foo SET bar = ?, baz = baz + ? WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          [1 2 "bar" 3 4 5 6 7]]
         (as-prepared (update :foo
                        (set-fields {:bar 1
                                     :baz [+ 2] })
                        (where {:foo :bar
                                :moo [> 3]
                                :meh [:> 4]
                                :baz [:in [5 6 7]]})))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2 WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (as-cql (update :foo
                   (set-fields {:bar 1
                                :baz [+ 2] })
                   (where {:foo :bar
                           :moo [> 3]
                           :meh [:> 4]
                           :baz [:in [5 6 7]]}))))))



(deftest test-delete
  (is (= ["DELETE * FROM foo USING TIMESTAMP 100000 AND TTL 200000 WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          ["bar" 3 4 5 6 7]]
         (as-prepared (delete :foo
                        (using :timestamp 100000
                               :ttl 200000)
                        (where {:foo :bar
                                :moo [> 3]
                                :meh [:> 4]
                                :baz [:in [5 6 7]]}))))))

(deftest test-truncate
  (is (= "TRUNCATE foo;"
         (as-cql (truncate :foo)))))

(deftest test-drop
  (is (= "DROP INDEX foo;"
         (as-cql (drop-index :foo))))

  (is (= "DROP KEYSPACE foo;"
         (as-cql (drop-keyspace :foo))))

  (is (= "DROP TABLE foo;"
         (as-cql (drop-table :foo)))))

(deftest test-create-index
  (is (= "CREATE INDEX ON foo ( bar );"
         (as-cql (create-index :foo :bar))))

  (is (= "CREATE INDEX baz ON foo ( bar );"
         (as-cql (create-index :foo :bar
                   (index-name "baz"))))))

(deftest test-batch
  (is (= "BATCH USING TIMESTAMP 2134 \nUPDATE foo SET bar = 1, baz = baz + 2;\nINSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;"
         (as-cql (batch
                   (queries
                    (update :foo
                      (set-fields {:bar 1
                                   :baz [+ 2] }))
                    (insert :foo
                      (values {"a" "b" "c" "d"})
                      (using :timestamp 100000
                             :ttl 200000)))
                   (using :timestamp 2134)))))

  (is (= ["BATCH USING TIMESTAMP 1234 \nUPDATE foo SET bar = ?, baz = baz + ?;\nINSERT INTO foo (a, c) VALUES (?, ?) USING TIMESTAMP 100000 AND TTL 200000;\n APPLY BATCH;" [1 2 "b" "d"]]
         (as-prepared (batch
                        (queries
                         (update :foo
                           (set-fields {:bar 1
                                        :baz [+ 2] }))
                         (insert :foo
                           (values {"a" "b" "c" "d"})
                           (using :timestamp 100000
                                  :ttl 200000)))
                        (using :timestamp 1234))))))

(deftest test-q->
  (let [q (select :foo)]
    (is (= "SELECT bar, baz FROM foo;")
        (as-cql (q-> q
                  (columns :bar "baz"))))

    (is (= ["SELECT ?, ? FROM ?;" ["foo" "bar" "baz"]])
        (as-prepared (q-> q
                       (columns :bar "baz")))))

  (let [q (insert :foo)
        q2 (q-> q
             (values {"a" "b" "c" "d"}))]
    (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd');"
           (as-cql q2)))
    (is (= "INSERT INTO foo (a, c) VALUES ('b', 'd') USING TIMESTAMP 100000 AND TTL 200000;"
           (as-cql (q-> q2
                     (using :timestamp 100000
                            :ttl 200000)))))))


;; (deftest test-functions
;;   (is (= "SELECT count(*) FROM foo;"
;;          (as-cql (select :foo
;;                    (columns (count*))))))

;;   (is (= ["SELECT count(*) FROM ?;" ["foo"]]
;;          (as-prepared (select :foo
;;                         (columns (count*))))))



;;   (is (= "SELECT * FROM foo WHERE ts = now();"
;;          (as-cql (select :foo
;;                    (where {:ts (now)})))))

;;   (is (= ["SELECT * FROM ? WHERE ? = now();" ["foo" "ts"]]
;;          (as-prepared (select :foo
;;                         (columns (count*))))))


;;   (is (= "SELECT * FROM foo WHERE token(user-id) > token('tom');"
;;          (as-cql (select :foo
;;                    (where {(token :user-id) [> (token "tom" true)]})))))

;;   (is (= ["SELECT * FROM ? WHERE token(?) > token(?);" ["foo" "user-id" "tom"]]
;;          (as-prepared (select :foo
;;                         (where {(token :user-id) [> (token "tom" true)]})))))


;;   )

;; (prn (token 1) )

;; (prn (as-prepared (select :foo
;;                (where {(token :user-id) [> (token "tom")]}))))