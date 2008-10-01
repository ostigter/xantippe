README
======


Introduction
------------

Xantippe is an open source, lightweight XML database written in Java featuring
validation, XQuery and indexing. It is written by Oscar Stigter and published
under the Apache License 2.0.

Project page: http://code.google.com/p/xantippe/


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

The first stage of development is complete; all basic functionality has been
implemented.

However, much more testing is required for Xantippe to be considered anywhere
near complete, stable and reliable. Please use at your own risk!
