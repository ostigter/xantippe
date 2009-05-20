README
======


Introduction
------------

Xantippe is an open source, lightweight XML database written in Java featuring
validation, XQuery and indexing. It is written by Oscar Stigter and published
under the Apache License 2.0.

Project page: http://xantippe.googlecode.com


Features
--------

- Documents (XML, plain text or binary) are stored in binary files on a file
  system, optionally using compression.

- Documents are hierarchically ordered in a tree of collections, similar to
  directories in a file system.

- XML documents can be automatically validated against their schema's.

- Documents can be quickly retrieved using indices based on document keys with
  XPath-like expressions. XML documents can be indexed automatically when
  inserted/updated based on their contents. Document keys can also be set
  manually, which is especially useful for non-XML documents.

- Validation, indices and compression are configured per collection using an
  inheritance tree.

- Queries, written in the XQuery language, can be exectued ad-hoc or using
  stored modules.

The XQuery functionality is powered by Saxon-B. Saxon is an open source XQuery
and XSLT processor written by Michael H. Kay from Saxonica Limited
(http://www.saxonica.com).


Status
------

The first stage of development is complete; all basic functionality (as
embedded library) has been implemented and pretty well-tested.

However, Xantippe is still in Alpha stage, so please use at your own risk!


Building
--------

Since the project adheres to the default Maven 2 structure, building it from
the command line is as easy as running "mvn clean install" (assuming you have
Maven installed). If using Eclipse, the 'Subversive' and 'm2eclipse' plugins
are highly recommended.

Unfortunately, because Saxon is not kept up to date in any public Maven
repository, you will have to download the latest version and deploy it to your
local repository with a command like:

    mvn install:install-file -Dfile=saxon9.jar -DgroupId=net.sf.saxon \
        -DartifactId=saxon -Dversion=9.1 -Dpackaging=jar
