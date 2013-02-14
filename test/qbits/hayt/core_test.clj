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

  (is (= ["SELECT * FROM %s WHERE foo = %s;" ["foo" "bar"]]
         (as-cql (-> (select :foo)
                     (where [])))))
  )