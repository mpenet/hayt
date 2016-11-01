(defproject cc.qbits/hayt "4.0.0-beta1"
  :description "CQL Query Generation"
  :url "https://github.com/mpenet/hayt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cc.qbits/commons "0.4.5"]
                 [org.apache.commons/commons-lang3 "3.4"]
                 [org.clojure/clojure "1.9.0-alpha12"]
                 ;; [org.clojure/test.check "0.9.0"]
                 ;; [cc.qbits/spex "0.1.1"]
                 ]
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :profiles {:1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :1.7 {:dependencies [[org.clojure/clojure "1.7.0-alpha2"]]}
             :master {:dependencies [[org.clojure/clojure "1.7.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[clj-time "0.8.0"]
                                   [codox "0.8.10"]]}}
  :aliases {"all" ["with-profile" "dev:dev,1.6:dev,1.7:dev,master"]}
  :codox {:src-dir-uri "https://github.com/mpenet/hayt/blob/master"
          :src-linenum-anchor-prefix "L"
          :output-dir "../hayt-gh/codox"
          :defaults {:doc/format :markdown}
          :include [qbits.hayt
                    qbits.hayt.dsl.statement
                    qbits.hayt.dsl.clause
                    qbits.hayt.fns
                    qbits.hayt.utils
                    qbits.hayt.codec.joda-time]}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :jvm-opts ^:replace ["-server"]
  :javac-options ["-source" "1.7" "-target" "1.7" "-g"]
  :global-vars {*warn-on-reflection* true})
