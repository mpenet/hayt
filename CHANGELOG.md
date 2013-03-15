# Changelog

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
