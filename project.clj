(defproject cc.qbits/hayt "1.0.0"
  :description "CQL Query Generation"
  :url "https://github.com/mpenet/hayt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.flatland/useful "0.10.0"]
                 [org.apache.commons/commons-lang3 "3.1"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6  {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[org.apache.cassandra/cassandra-all "1.2.3"]
                                   [clj-time "0.5.0"]]}
             :test {:dependencies [[org.apache.cassandra/cassandra-all "1.2.3"]
                                   [clj-time "0.5.0"]]}}
  :codox {:src-dir-uri "https://github.com/mpenet/hayt/blob/master"
          :src-linenum-anchor-prefix "L"
          :output-dir "../hayt-gh/codox"
          :exclude [qbits.hayt.cql
                    qbits.hayt.dsl
                    qbits.hayt.utils
                    qbits.hayt.fns]}
  :warn-on-reflection true)
