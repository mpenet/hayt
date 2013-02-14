(ns qbits.hayt.core-test
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

  (is (= ["SELECT * FROM %s WHERE %s = %s AND %s > %s AND %s > %s AND %s > %s IN (%s, %s, %s);"
          ["foo" "foo" "bar" "moo" 3 "meh" 4 "baz" 5 6 7]]
         (as-cql (-> (select :foo)
                     (where {:foo :bar
                             :moo [> 3]
                             :meh [:> 4]
                             :baz [:in [5 6 7]]})))))


  (is (= ["INSERT INTO %s (%s, %s) VALUES (%s, %s) USING TIMESTAMP %s AND TTL %s;"
          ["foo" "a" "c" "'b'" "'d'" 100000 200000]]
         (as-cql (-> (insert :foo)
                     (values {"a" "b" "c" "d"})
                     (using :timestamp 100000
                            :ttl 200000))))))