# Hayt [![Build Status](https://secure.travis-ci.org/mpenet/hayt.png?branch=master)](http://travis-ci.org/mpenet/hayt)

CQL3 DSL for Clojure.

## Why?

There are a number of clients available for Cassandra for clojure and
none provides a CQL3 DSL yet.

Hayt is an attempt to allow the existing and future clients to share
this, provide a simple but extensible base to build upon.

Our goals from the start were to be feature complete (up to CQL
v3.0.2), idiomatic, well tested, and performant.

There are already 3 libraries that will use Hayt:

* [mpenet/alia](https://github.com/mpenet/alia)
* [clojurewerkz/cassaforte](https://github.com/clojurewerkz/cassaforte)
* [mpenet/casyn](https://github.com/mpenet/casyn)

Hayt development is the result of collaboration, thanks to our [contributors](https://github.com/mpenet/hayt/contributors).

## Installation

```clojure
[cc.qbits/hayt "0.3.0-RC1"]
```

## Usage

This should be familiar if you know Korma or ClojureQL.
One of the major difference is that Hayt doesn't use macros.

Some examples:

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
                 :baz [:in [5 6 7]]})
         (order-by [:foo :asc]))
```

Queries are composable using `q->`

```clojure
(def base (select :foo (where {:foo 1})))

(q-> base
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

```

If you are curious about what else it can do head to the [very
incomplete documentation](http://mpenet.github.com/hayt/codox/qbits.hayt.html) or the [tests](https://github.com/mpenet/hayt/blob/master/test/qbits/hayt/core_test.clj).

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

Copyright © 2013 mpenet

Distributed under the Eclipse Public License, the same as Clojure.
