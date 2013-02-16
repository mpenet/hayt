(ns qbits.hayt.core-test
  (:use clojure.test
        qbits.hayt
        qbits.hayt.cql))

(deftest test-select

  (is (= "SELECT * FROM foo;"
         (->cql (select :foo))))


  (is (= "SELECT bar, baz FROM foo;"
         (->cql (select :foo
                        (columns :bar "baz")))))

  (is (= "SELECT bar, baz FROM foo LIMIT 100;"
         (->cql (select :foo
                        (columns :bar "baz")
                        (limit 100)))))

  (is (= "SELECT * FROM foo ORDER BY bar desc;"
         (->cql (select :foo
                        (order-by [:bar :desc])))))

  ;;
  (is (= ["SELECT * FROM foo WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          ["bar" 3 4 5 6 7]]
         (->prepared (select :foo
                             (where {:foo :bar
                                     :moo [> 3]
                                     :meh [:> 4]
                                     :baz [:in [5 6 7]]})))))

  (is (= "SELECT * FROM foo WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (->cql (select :foo
                        (where {:foo :bar
                                :moo [> 3]
                                :meh [:> 4]
                                :baz [:in [5 6 7]]}))))))

  (is (= "SELECT * FROM foo WHERE foo > 1 AND foo < 10;"
         (->cql (select :foo
                        (where [[:foo  [> 1]]
                                [:foo  [< 10]]])))))

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
  ;;
  (is (= ["UPDATE foo SET bar = ?, baz = baz + ?;" [1 2]]
         (->prepared (update :foo
                             (set-columns {:bar 1
                                           :baz [+ 2] })))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2;"
         (->cql (update :foo
                        (set-columns {:bar 1
                                      :baz [+ 2] })))))

  (is (= ["UPDATE foo SET bar = ?, baz = baz + ? WHERE foo = ? AND moo > ? AND meh > ? AND baz IN (?, ?, ?);"
          [1 2 "bar" 3 4 5 6 7]]
         (->prepared (update :foo
                             (set-columns {:bar 1
                                           :baz [+ 2] })
                             (where {:foo :bar
                                     :moo [> 3]
                                     :meh [:> 4]
                                     :baz [:in [5 6 7]]})))))

  (is (= "UPDATE foo SET bar = 1, baz = baz + 2 WHERE foo = 'bar' AND moo > 3 AND meh > 4 AND baz IN (5, 6, 7);"
         (->cql (update :foo
                        (set-columns {:bar 1
                                      :baz [+ 2] })
                        (where {:foo :bar
                                :moo [> 3]
                                :meh [:> 4]
                                :baz [:in [5 6 7]]}))))))



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
  (is (= "DROP INDEX foo;"
         (->cql (drop-index :foo))))

  (is (= "DROP KEYSPACE foo;"
         (->cql (drop-keyspace :foo))))

  (is (= "DROP TABLE foo;"
         (->cql (drop-table :foo)))))

(deftest test-create-index
  (is (= "CREATE INDEX ON foo (bar);"
         (->cql (create-index :foo :bar))))

  (is (= "CREATE INDEX baz ON foo (bar);"
         (->cql (create-index :foo :bar
                              (index-name "baz"))))))

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

(deftest test-create-alter-keyspace
  (is (= "CREATE KEYPACE foo WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
         (->cql (create-keyspace :foo
                  (with {:replication
                         {:class "SimpleStrategy"
                          :replication_factor 3 }})))))

  (is (= "ALTER KEYPACE foo WITH something-else = 'foo' AND something = 1 AND replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};"
         (->cql (alter-keyspace :foo
                                (with {:replication
                                       {:class "SimpleStrategy"
                                        :replication_factor 3 }
                                       :something 1
                                       :something-else "foo"})))))
  )

(deftest test-q->
  (let [q (select :foo)]
    (is (= "SELECT bar, baz FROM foo;")
        (->cql (q-> q
                    (columns :bar "baz"))))

    (is (= ["SELECT ?, ? FROM ?;" ["foo" "bar" "baz"]])
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
  (is (= "SELECT COUNT(*) FROM foo;"
         (->cql (select :foo (columns (count*))))))

  (is (= "SELECT * FROM foo WHERE ts = now();"
         (->cql (select :foo
                        (where {:ts (now)})))))

  (is (= ["SELECT * FROM foo WHERE ts = now();" []]
         (->prepared (select :foo
                             (where {:ts (now)})))))

  (is (= "SELECT WRITETIME(bar) FROM foo;"
         (->cql (select :foo (columns (writetime "bar"))))))

  (is (= "SELECT TTL(bar) FROM foo;"
         (->cql (select :foo (columns (ttl "bar"))))))

  (is (= "SELECT unixTimestampOf(bar), dateOf(bar) FROM foo;"
         (->cql (select :foo (columns (unix-timestamp-of "bar")
                                      (date-of "bar"))))))

  (is (= "SELECT * FROM foo WHERE token(user-id) > token('tom');"
         (->cql (select :foo
                        (where {(token :user-id) [> (token "tom")]})))))

  (is (= ["SELECT * FROM foo WHERE token(user-id) > token(?);" ["tom"]]
         (->prepared (select :foo
                             (where {(token :user-id) [> (token "tom")]})))))

  (deftest test-coll-lookup
    (is (= "DELETE bar[2] FROM foo WHERE baz = 1;"
           (->cql (delete :foo
                          (columns {:bar 2})
                          (where {:baz 1})))))
    (is (= ["DELETE bar[?] FROM foo WHERE baz = ?;" [2 1]]
           (->prepared (delete :foo
                               (columns {:bar 2})
                               (where {:baz 1})))))))

;; FIXME Locale issues, maybe the approach is just wrong
;; (let [d (java.util.Date. 0)
;;       ds (str (.format uuid-date-format d))]
;;   (is (= "SELECT * FROM foo WHERE ts > maxTimeuuid('1970-01-01 01:00+0100') AND ts < minTimeuuid('1970-01-01 01:00+0100');"
;;          (cql (select :foo
;;                    (where [[:ts  [> (max-timeuuid d)]]
;;                            [:ts  [< (min-timeuuid d)]]]))))))
;; )