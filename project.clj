(defproject cc.qbits/hayt "0.4.0-beta2"
  :description "CQL Query Generation"
  :url "https://github.com/mpenet/hayt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [core.typed "0.1.7"]
                 [useful "0.8.8"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :1.6  {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}
             :test {:dependencies []}
             :dev {:dependencies [[criterium "0.3.1"]]}}
  :codox {:src-dir-uri "https://github.com/mpenet/hayt/blob/master"
          :src-linenum-anchor-prefix "L"
          :exclude [qbits.hayt.cql
                    qbits.hayt.dsl
                    qbits.hayt.utils
                    qbits.hayt.fns]}
  :warn-on-reflection true)
