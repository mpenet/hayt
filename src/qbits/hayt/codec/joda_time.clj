(ns qbits.hayt.codec.joda-time
  (:require
   [qbits.hayt.cql :as cql]
   [clj-time.coerce :as ct]))

(extend-protocol cql/CQLEntities
  org.joda.time.DateTime
  (cql-value [x]
    (ct/to-long x)))
