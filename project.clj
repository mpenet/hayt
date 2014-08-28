(defproject cc.qbits/hayt "2.1.0-SNAPSHOT"
  :description "CQL Query Generation"
  :url "https://github.com/mpenet/hayt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cc.qbits/pod "0.1.0"]
                 [org.flatland/useful "0.10.1"]
                 [org.apache.commons/commons-lang3 "3.3.2"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :1.7  {:dependencies [[org.clojure/clojure "1.7.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[clj-time "0.8.0"]
                                   ;; [com.taoensso/timbre "3.2.1"]
                                   ]}
             :test {:dependencies [[clj-time "0.8.0"]]}}
  :codox {:src-dir-uri "https://github.com/mpenet/hayt/blob/master"
          :src-linenum-anchor-prefix "L"
          :output-dir "../hayt-gh/codox"
          :include [qbits.hayt
                    qbits.hayt.dsl.statement
                    qbits.hayt.dsl.clause
                    qbits.hayt.fns
                    qbits.hayt.utils
                    qbits.hayt.codec.joda-time]}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-source" "1.6" "-target" "1.6" "-g"]
  :global-vars {*warn-on-reflection* true})
