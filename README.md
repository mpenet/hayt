# Hayt [![Build Status](https://secure.travis-ci.org/mpenet/hayt.png?branch=master)](http://travis-ci.org/mpenet/hayt)

CQL query generation from Clojure.

## Why?

There are a number of clients available for Cassandra for clojure and
none provides CQL3 generation yet.

Hayt is an attempt to allow the existing and future clients to share
this, provide a simple but extensible base to build upon.

Our goals from the start where to be feature complete, idiomatic, well
tested, and performant.

There are already 3 libraries that will use this library:

* [mpenet/alia](https://github.com/mpenet/alia)
* [clojurewerkz/cassaforte](https://github.com/clojurewerkz/cassaforte)
* [mpenet/casyn](https://github.com/mpenet/casyn)

Hayt development is the result of collaboration, thanks to our [contributors](https://github.com/mpenet/hayt/contributors).

## Installation

```clojure
[cc.qbits/hayt "0.1.0-SNAPSHOT"]
```

This is still a SNAPSHOT version, but we should have a proper release
in a matter of days, once documentation and testing are completed.

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
incomplete documentation](http://mpenet.github.com/hayt/qbits.hayt.html).


## License

Copyright © 2013 mpenet

Distributed under the Eclipse Public License, the same as Clojure.
