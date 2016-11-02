(defproject cc.qbits/hayt "4.0.0-beta4"
  :description "CQL Query Generation"
  :url "https://github.com/mpenet/hayt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cc.qbits/commons "0.4.6"]
                 [org.apache.commons/commons-lang3 "3.4"]
                 [org.clojure/clojure "1.9.0-alpha14"]]
  :profiles {:dev  {:dependencies [[clj-time "0.8.0"]
                                   [codox "0.8.10"]]}}

  :codox {:source-uri "https://github.com/mpenet/hayt/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jvm-opts ^:replace ["-server"]
  :javac-options ["-source" "1.7" "-target" "1.7" "-g"]
  :global-vars {*warn-on-reflection* true})
