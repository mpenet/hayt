# Changelog

## 3.1.0

* Add new cql functions

## 3.0.1

* add missing entry point for `alter-type`. Thanks @acron0 -> https://github.com/mpenet/hayt/pull/39

## 3.0.0

* auto-extend alia protocols if alia is detected

## 3.0.0-rc2

* allow any sequential in counter/collection ops

## 3.0.0-rc1

* upgrade to clj 1.7, removed/updated some dependencies

* Performance improvements (raw speed, mem, allocations)

* Remove prepared statements generation with values
qbits.hayt/->prepared since it's used by nobody now.  You can still
use the ? placeholer and actually control what's happening
ex: `(select :foo (where {:id ?}))`.


## 2.1.0

* Add 2.1+ udt support, `qbits.hayt/frozen` see tests

## 2.0.0

Release 2.0.0 (check past logs for changes)

## 2.0.0-rc4

* nil queries are ignored in `batch`, see #30

## 2.0.0-rc3

* no longer depend on flatland/useful

## 2.0.0-rc1 - Cassandra 2.1 features

* Add `qbits.hayt/create-type` statement and `qbits.hayt/user-type` type marker for UDT values
* Slightly improved perf/ressource use
* Add tests for UDT litterals

## 2.0.0-beta4

* use clojure 1.6

* `where1`, `using`, `set-columns`, `values`, `with` now supports
  cassaforte like signatures (unspliced map `(where1 :a 1 :b 2)` or
  `(where1 {:a 1
  :b 2})`}

## 2.0.0-beta3 - **Breaking changes (backward compatible fns available)**

* new `where` syntax, matches clojure style more closely (prefix
  notation), it now expects a seq of seq:

  ```clojure
  (where [[= :a 1] [> :b 2]])
  ```

  It can still receive a map, but it will only assume =, a vector of 2
  elements will assume =.

```clojure
  (where {:a 1 :b 2})
```

* `where'` same as `where` but allows to skip passing the wrapping vector:
```clojure
(where' [= :b 2] [> :c 3])
```

* `where1` is the equivalent of the previous `where` syntax from v1
  and early v2 betas. ex: `(where1 {:a [> 2]}}`

* Add support for paging on composite in `where`

```clojure
(where [[= :a 1]
        [>= [:b :c :d] [1 2 3]]])
```

* qbits.hayt/distinct* is now variadic, thanks @pyr

```clojure
(select :foo (columns (distinct* :foo :bar :baz)))
```

* Add support for secondary index on collections in `where` CASSANDRA-4511

```clojure
(where [contains :l 1]}
```

```clojure
(where [contains-key :m 2]}
```

* Add support for static columns in schemas

* Add sugar for operations on collections and counters

```clojure
(update :foo
        (set-columns {:bar 1
                      :baz (inc-by 2)}))

(update :foo
        (set-columns {:bar 1
                      :baz (dec-by 2)}))

(update :foo
        (set-columns {:baz (prepend ["asdf"])})
        (where [[:foo "bar"]]))

(update :foo
        (set-columns {:baz (append ["asdf"])})
        (where [[:foo "bar"]]))
```

## 2.0.0-beta2

Add support for vectors in set-columns and values. Ex: `(insert :foo (values [[:a 1] [:b 2]]))`
See Issue #19 for details.

## 2.0.0-beta1

Some changes require you to run against Cassandra 2.0+ !!

### Breaking changes!

* Keywords as values are no longer encoded as strings and return a
  placeholder for a named prepared statement value. See #18

  The following
  ```clj
  (->raw (select :foo (where {:bar :baz})))
  ```
  used to generate this:
  ```"SELECT FROM foo WHERE bar = 'baz';"```
  and now this:
  ```"SELECT FROM foo WHERE bar = :baz;"```

* When using `->prepared` the values for `using` and `limit` are now
  parameterized.

* The `IN` operator in where clauses when using `->prepared` is now
  variadic (it returns the values as an array in the parameter list
  and uses a single ? placeholder). This is not backward compatible with C* 2.0-

```clj
(where {:foo [:in [1 2 3]]}) -> ["...WHERE foo IN ?;" [[1 2 3]]]
```

* `apply-map` has been removed from `qbits.hayt.utils` (it didn't make
  sense since we now have proper named prepared statement
  placeholders).

## 1.4.1

* fix `IF NOT EXISTS` clauses in create/drop statements
* add drop-columnfamily, alter-columnfamily
* Moved `?` into utils.clj

## 1.4.0

* Added `qbits.hayt.fns/?` to be used as prepared statement value
  placeholder when using ->raw to generate prepared statements.

## 1.3.0

* Added `create-trigger`, `drop-trigger`
* Added keyspace qualified identifiers (ex: "select * from mykeyspace.mytable;")
* fix blob->X functions, they now accept bytebuffers and byte array as
  input and properly encode them

## 1.2.0

* Support up to cql 3.1.1 http://cassandra.apache.org/doc/cql3/CQL.html#a3.1.1
** Added `qbits.hayt.clause/if-exists`
** Added `qbits.hayt.fns/distinct*`
** `qbits.hayt.clause/if-not-exists` is deprecated, use
   `(qbits.hayt.clause/if-exists false)`
** minor code cleanups in `qbits.hayt.cql`

## 1.1.4

* Preserve ns in when using namespaced keywords as values

## 1.1.3

* `qbits.hayt.fns/token` is now variadic, allowing composite token values

## 1.1.2

* Dropped dependency on clj 1.5

## 1.1.1

* Dropped dependency on cassandra-all, no api changes

## 1.1.0

* Add encoding support for byte array values

## 1.0.5

* Allow :primary-key :abc (value not wrapped in a vector)

## 1.0.4

* Minor perf. improvements

## 1.0.3

* ns change: `qbits.hayt.dsl.verb` becomes `qbits.hayt.dsl.statement`.
  This should be backward compatible as long as you don't use them
  directly, the aliases have been updated.

## 1.0.2

* Fix Collection types definitions (they were quoted)

* Fix DELETE * statement :emit (no * on delete, it now accepts nil column or :*)

## 1.0.1

* Internals change to accomodate cassaforte

## 1.0.0

* Exposed `cql-raw` and `cql-fn`, the former allows to pass arbitrary raw
  content as value, and the later allows to create function wrappers.
  These two could be usefull when/if we don't support a feature and/or
  you need lower level access to value/fn encoding.

* Added alias support

* Added ALTER DROP clause support

* tests, docstrings improvements

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
