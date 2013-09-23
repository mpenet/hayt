# Hayt [![Build Status](https://secure.travis-ci.org/mpenet/hayt.png?branch=master)](http://travis-ci.org/mpenet/hayt)

CQL3 DSL for Clojure.

## Why?

Hayt is an attempt to allow the existing and future clients to share
this layer and provide a simple but extensible base to build upon.

There are already 3 libraries that use Hayt:

* [mpenet/alia](https://github.com/mpenet/alia)
* [clojurewerkz/cassaforte](https://github.com/clojurewerkz/cassaforte)
* [mpenet/casyn](https://github.com/mpenet/casyn)

## What's in the box?

* **Complete CQL 3.1.1+ coverage** including some features in Cassandra
  trunk, **DDL**, **CQL Functions**, **counter** , **triggers** and
  **collections** operations
* Support for both **Raw queries** and **Prepared Statements** generation
* **Great performance**
* **Extensive test coverage**
* Decoupled query compiler, allowing you to **build your own DSL** in minutes
* Highly **composable** using simple maps or the functions provided
* Extensible **Clojure data types support**
* Constantly **kept up to date** with Cassandra changes, almost daily
* No macros, no pain

## Installation

```clojure
[cc.qbits/hayt "1.3.0"]
```

## Usage

This should be familiar if you know Korma or ClojureQL.
One of the major difference is that Hayt doesn't exposes macros.


```clojure

(use 'qbits.hayt)

(select :foo
        (where {:bar 2}))

(update :some-table
         (set-columns {:bar 1
                       :baz [+ 2]})
         (where {:foo :bar
                 :moo [> 3]
                 :meh [:> 4]
                 :baz [:in [5 6 7]]}))
```

All these functions do is generate maps, if you want to build your own
DSL on top of it, or use maps direcly feel free to do so.

```clojure
(select :users
        (columns :a :b)
        (where {:foo :bar
                :moo [> 3]
                :meh [:> 4]
                :baz [:in [5 6 7]]}))

;; generates the following

>> {:select :users
    :columns [:a :b]
    :where {:foo :bar
            :moo [> 3]
            :meh [:> 4]
            :baz [:in [5 6 7]]}}
```

Since Queries are just maps they are composable using the usual `merge`
`into` `assoc` etc.

```clojure
(def base (select :foo (where {:foo 1})))

(merge base
       (columns :bar :baz)
       (where {:bar 2})
       (order-by [:bar :asc])
       (using :ttl 10000))

```

To compile the queries just use `->raw` or `->prepared`

```clojure
(->raw (select :foo))
> "SELECT * FROM foo;"

(->prepared (select :foo (where {:bar 1})))
> ["SELECT * FROM foo WHERE bar=?;" [1]]


(->raw {:select :foo :columns [:a :b]})
> "SELECT a, b FROM foo;"

;; or if you want full control of what's prepared you can use
   `cql-raw` with ->raw compilation:


;; here `?` is an alias to `(cql-raw "?")`

(->raw (select :foo (where {:bar 1 :baz ?})))
> "SELECT * FROM foo WHERE bar = 1 AND baz = ?;"


;; and named parameters using cql-raw directly:

(->raw (select :foo (where {:bar 1 :baz (cql-raw :named)})))
> "SELECT * FROM foo WHERE bar = 1 AND baz = :named;"


```

When compiling prepared queries, the values are untouched in the
returned vector.
When compiling with `->raw` we take care of the encoding/escaping
required by CQL. This process is also open via the
`qbits.hayt.CQLEntities` protocol for both values and identifiers. We
also supply a joda-time codec for you to use if you require to in
`qbits.hayt.codec.joda-time`. This codec is a good example of how to
handle custom type encoding.

If you are curious about what else it can do head to the
[codox documentation](http://mpenet.github.com/hayt/codox/qbits.hayt.html)
or the
[tests](https://github.com/mpenet/hayt/blob/master/test/qbits/hayt/core_test.clj).

## Hayt?

Nope, it doesn't stand for "How Are You Today" :)

Hayt is a gola, a manufactured clone in
[Dune](http://en.wikipedia.org/wiki/Dune_universe), made from cells of
a (deceased) person,
[Duncan Idaho](http://en.wikipedia.org/wiki/Duncan_Idaho) in this
instance.
Long (and complex) story short, he eventually becomes the husband of
[Alia](http://en.wikipedia.org/wiki/Alia_Atreides).

## License

Copyright © 2013 [Max Penet](https://twitter.com/mpenet)

Distributed under the Eclipse Public License, the same as Clojure.
