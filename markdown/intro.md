# Hayt

## Identifiers vs values

CQL identifiers (table names, column names, etc) can be represented
as clojure keywords or as strings.  In the case of a keyword it
wont be quoted, meaning it will be case insensitive and cannot include
spaces. In string form, it will be quoted, can include spaces and will
be case sensitive, as definied in the CQL reference.

[Identifiers and keywords](http://cassandra.apache.org/doc/cql3/CQL.html#identifiers)

## Verbs & clauses

Queries are composed of a verb and a list of clauses. Since many verbs
share clauses of the same type, we will start by listing the verbs
available, and link to the clause they have access to and their
description on the second part of this document.

### Verbs

#### Select

The `select` fn takes a table name and a list of optional clauses:

 * [columns](#columns)
 * [where](#where)
 * [order-by](#order-by)
 * [limit](#limit)
 * [allow filtering](#allow-filtering)

Ex:
```clojure
(select :foo
        (columns :foo :moo :baz :meh)
        (where {:foo :bar
                :moo [> 3]
                :meh [:> 4]
                :baz [:in [5 6 7]]}))
```

#### Insert

The `insert` fn takes a table name and a list of optional clauses:

 * [values](#values)
 * [using](#using)

Ex:
```clojure
(insert :foo
        (values {"c" "d" :a "b" })
        (using :timestamp 100000
               :ttl 200000))
```

#### Update

The `update` fn takes a table name and a list of optional clauses:

 * [set-columns](#set-columns)
 * [where](#where)
 * [using](#using)

Ex:
```clojure
(update :foo
        (set-columns {:bar 1
                      :baz [+ 2]})
        (where {:foo :bar
                :moo [> 3]
                :meh [:> 4]
                :baz [:in [5 6 7]]}))
```

#### Delete

The `delete` fn takes a table name and a list of optional clauses:

 * [columns](#columns)
 * [where](#where)
 * [using](#using)

Ex:
```clojure
(delete :foo
        (using :timestamp 100000
               :ttl 200000)
        (where {:foo :bar
                :moo [> 3]
                :meh [:> 4]
                :baz [:in [5 6 7]]}))
```

#### Truncate

The `truncate` fn takes a table name.

Ex:
```clojure
(truncate :foo)
```

#### Drop Keyspace

The `drop-keyspace` fn takes a keyspace name.

Ex:
```clojure
(drop-keyspace :keyspace-foo)
```


#### Drop Table

The `drop-table` fn takes a table name.

Ex:
```clojure
(drop-table :foo)
```

#### Drop Index

The `drop-index` fn takes an index name.

Ex:
```clojure
(drop-index :foo-index)
```

#### Create Index

The `create-index` fn takes a table name, a column identifier and an
optional index-name clause.

* [index-name](#index-name)

Ex:
```clojure
(create-index :foo :bar
              (index-name "baz")))
```

#### Create Keyspace

The `create-keyspace` fn takes a keyspace name and a `with` clause.

* [with](#with)

Ex:
```clojure
(create-keyspace :foo
                 (with {:replication
                        {:class "SimpleStrategy"
                         :replication_factor 3 }}))
```

#### Create Table

The `create-table` fn takes a table name and a list of clauses.

* [with](#with)
* [column-definitions](#column-definitions)

Ex:
```clojure
(create-table :foo
              (column-definitions {:foo :varchar
                                   :bar :int
                                   :primary-key [:foo :bar]})
              (with {:compact-storage true
                     :clustering-order [[:bar :asc]]}))
```

#### Alter Table

The `alter-table` fn takes a table name and a list of clauses.

* [with](#with)
* [alter-column](#alter-column)
* [add](#add)

Ex:
```clojure
(alter-table :foo
             (alter-column :bar :int)
             (add :baz :text)
             (with {:compact-storage true
                    :clustering-order [[:bar :asc]]}))
```


#### Alter Column Family

The `alter-column-family` fn takes a column family name and a list of clauses.

* [with](#with)
* [alter-column](#alter-column)
* [add](#add)

Ex:
```clojure
(alter-column-family :foo
                     (alter-column :bar :int)
                     (add :baz :text)
                     (with {:compact-storage true
                            :clustering-order [[:bar :asc]]}))
```

#### Alter Keyspace

The `alter-keyspace` fn takes a keyspace name and a `with` clause

* [with](#with)

Ex:
```clojure
(alter-column-family :foo
                     (with {:compact-storage true
                            :clustering-order [[:bar :asc]]}))
```

#### Batch

The `batch` fn takes a list of hayt queries in a `queries` clause and an
optional `using` clause.

* [using](#using)
* [queries](#queries)

Ex:
```clojure
(batch
 (queries
   (update :foo
            (set-columns {:bar 1
                          :baz [+ 2] }))
   (insert :foo
           (values {"a" "b" "c" "d"})
           (using :timestamp 100000
                  :ttl 200000)))
  (using :timestamp 2134))
```



#### Use Keyspace

The `use-keyspace` fn takes a keyspace name.

Ex:
```clojure
(use-keyspace :foo)
```


#### Grant

The `grant` fn takes a permission identifier and clauses:

* [resource](#resource)
* [user](#user)

Ex:
```clojure
(grant :ALL
       (resource :bar)
       (user :user-foo))
```

#### Revoke

The `revoke` fn takes a permission identifier and clauses:

* [resource](#resource)
* [user](#user)

Ex:
```clojure
(revoke :ALL
       (resource :bar)
       (user :user-foo))
```

#### create-user

The `create-user` fn takes clauses:

* [password](#password)
* [superuser](#superuser)
* [user](#user) (optionaly using composition)

Ex:
```clojure
(create-user :foo
             (password :bar)
             (superuser true))
```

#### alter-user

The `alter-user` fn takes clauses:

* [password](#password)
* [superuser](#superuser)
* [user](#user) (optionaly using composition)

Ex:
```clojure
(alter-user :foo
            (password :bar)
            (superuser true))
```

#### drop-user

The `drop-user` fn takes a user name:

Ex:
```clojure
(drop-user :foo)
(drop-user "bar")
```


#### list-users

The `list-users` takes no argument

Ex:
```clojure
(list-users)
```

#### list-perm

The `list-perm` fn takes clauses:

* [perm](#perm)
* [resource](#resource)
* [user](#user)
* [recursive](#recursive) (defaults to true)

Ex:
```clojure
(list-perm (perm :ALTER)
           (resource :bar)
           (user :baz)
           (recursive false))
```


### Clauses

#### where

`where` takes a map, or a sequence of pairs.
The map keys will be the column identifiers.
The values can take different forms:
* If it's a simple value, it will generate an `=` match with the key
* If it's a vector, the first value will be an operator and the second its value.
  Operators supported are: `=` `>` `<` `>=` `<=`, keywords are also
  accepted. `:in ` will have a sequential value, that will generate an
  IN sequence.

Ex:

```clojure
(where {:foo :bar
        :moo [> 3]
        :meh [:> 4]
        :baz [:in [5 6 7]]})
```

The second case (sequence of pairs) that allow multiple checks on the
same column:

```clojure
(where [[:foo [> 1]]
        [:foo [< 100]]])
```

#### columns

`columns` is quite simple, it just accepts column identifiers to be
returned by rows. When ommited `*` will take its place.

Ex:
```clojure
(columns :foo "abc" "bar" :baz)
```

#### using

`using` takes a sequence of key values as cql options:

the options available are `ttl` and `timestamp`.

```clojure
(using :ttl 1000 :timestamp 1234545345345)
```

#### limit

The LIMIT option to a statement limits the number of rows returned by a query.

`limit` takes a number.

```clojure
(limit 1000)
```

#### order-by

The ORDER BY option allows to select the order of the returned results.

`order-by` takes vectors of 2 elements, where the first is the column
identifier and the second is the ordering as keyword as :desc or :asc
(case doesn't matter for the ordering).
(order-by [:foo :desc] ["bar" :asc])

#### column-definitions

#### queries

`queries` can only be used with batch, and takes the queries that will
be sent with the parent `batch`

```clojure
 (queries
   (update :foo
            (set-columns {:bar 1
                          :baz [+ 2] }))
   (insert :foo
           (values {"a" "b" "c" "d"})
           (using :timestamp 100000
                  :ttl 200000)))
```


#### values

#### set-columns

#### with

#### index-name

#### alter-column

#### add-column

#### rename-column

#### allow-filtering

#### logged

#### counter

#### resource

#### user

#### superuser

#### password

#### perm

#### recursive

#### table

#### column-family

#### keyspace


## Functions

### count(*) & count(1)

### now

### token

### max-timeuuid

### min-timeuuid

### writetime

### ttl

### unix-timestamp-of

### date-of

### blob->type

### type->blob

### blob->bigint

### bigint->blob

## Query generation

Hayt won't generate the query until you ask for it explicitly.
This has a few advantages, you can compose your queries using the
`q->` function at will until you generate them.

```clojure
(def base (select :foo (where {:foo 1})))

(def qa (q-> base
             (columns :bar :baz)
             (where {:bar 2})))

(def qb (q-> base
             (order-by [:bar :asc])
             (using :ttl 10000)
             (columns :bar :baz :bal)))
```

### Raw
Query generation can be achieved in two ways, if you need the raw
query with the values hardcoded you can then call `->raw` on it.

```clojure
(->raw (select :foo (where :bar 1)) )
> "SELECT * FROM foo WHERE bar=1;"
```

### Prepared

But you can also have them setup to be used as prepared statements, in
this case it will return a vector with the first values being the
query with values replaced by `?` and the second value the parameters
for later binding.

```clojure
(->prepared (select :foo (where {:bar 1})))
> ["SELECT * FROM foo WHERE bar=?;" [1]]
```

#### apply-map
But you don't want to have to deal with the index of
an argument in the list of parameters to feed it to your client lib.,
a way to prevent this is using `apply-map`.

It takes a generated prepared query with its arg vector containing
keywords for value placeholders and maps the supplied clojure map to it.

ex:
```clojure
(def query (->prepared (select :foo
                                (where {:a :a-placholder
                                        :b :b-placeholder
                                        :c :c-placeholder}))))

(println (apply-map query {:a-placholder 100
                           :b-placeholder 200
                           :c-placeholder 300}))

>> ["SELECT * FROM foo WHERE a = ? AND c = ? AND b = ?;" [100 300 200]]
```


### Sugar for collection type definitions

These can be used with `column-definitions` and `alter-column`.

```clojure
(map-type :int :text)
>> "map<int, text>"

(set-type :int)
>> "set<int>"

(list-type :int)
>> "list<int>"
```
