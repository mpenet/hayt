# Changelog

## X.X.X

* Exposed `cql-raw` and `cql-fn`, the former allows to pass arbitrary raw
  content as value, and the later allows to create function wrappers.
  These two could be usefull when/if we don't support a feature and/or
  you need lower level access to value/fn encoding.

* Added alias support

* Added ALTER DROP clause support

## 0.5.1

* Joda time support

* Moved cassandra-all in dev/test profiles

## 0.5.0

* CQL 3.0.3 support (create secondary index aka "CREATE CUSTOM INDEX")

* Added CAS support: `if-not-exists` `only-if` clauses
  https://issues.apache.org/jira/browse/CASSANDRA-5443

* Performance/ressource use improvements

* Added support for null values

* Added support for Date, blob (ByteBuffer instances), InetAddress literals

* Added noarg versions of bool clauses

* Completed/fixed coverage of blobAsX and XAsBlob functions

## 0.4.0-beta4

* Lift restriction on nil values (supported in C* 1.2.4+)

## 0.4.0-beta3

* Deprecate `q->`, since clauses/queries are just maps, use the usual
  `merge`/`assoc`/`into`. It will be removed in a future version likely 1.0.0

* Allow to use lowercased/dashed kw for permission, ex: `:full-access`
  or `:FULL_ACCESS`

* Fix `:primary-key` to allow composites

## 0.4.0-beta2

* Fix all permission queries, grant, revoke, list-permission (missing
  PERMISSION or PERMISSIONS)

* Renamed list-permission to list-perm

## 0.4.0-beta1

* cleaner AST and document (briefly) it's use in the readme
* renamed/updated the auth functions/clauses to match their cql
  counterpart more closely.

## 0.3.0

* Renamed `add` clause to `add-column` (breaking change)

* Fixed bug in batch clause (begin missing), and add missing arguments
  (UNLOGGED, COUNTER)

* Added `rename-column` `grant-user`, `revoke-user`, `alter-user`, `create-user`,
`drop-user`, `list-users`, `list-permissions`

* Added the following clauses `table`, `keyspace`, `column-family`,
  `logged`, `counter`, `resource`, `user`, `superuser`, `password`,
  `permission`, `recursive`.
