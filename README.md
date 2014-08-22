osta
----

[![Build Status](https://travis-ci.org/ostensus/osta.png?branch=master)](https://travis-ci.org/Ostensus/osta)

`osta` connects datasources behind the firewall to Ostensus.

Installing
----------

	$ npm install

Dependencies
------------

To run the integration test suite, you need have the `ostn` server running:

    $ go get github.com/ostensus/ostn
    $ $GOROOT/bin/ostn &

Testing
--------

	$ npm install -g mocha
	$ mocha --ui tdd

License
-------

osta is licensed under the terms of the Mozilla Public License, v. 2.0.
