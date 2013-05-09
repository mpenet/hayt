(ns qbits.hayt.codec.joda-time
  "require/use this namespace if you want hayt to support Joda DateTime
encoding when generating raw queries"
  (:require
   [qbits.hayt.cql :as cql]
   [clj-time.coerce :as ct]))

(extend-protocol cql/CQLEntities
  org.joda.time.DateTime
  (cql-value [x]
    (ct/to-long x)))
