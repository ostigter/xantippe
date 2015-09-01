### Introduction ###

Xantippe is a lightweight XML database featuring validation, indexing and XQuery (powered by Saxon). It is completely written in Java.


### News ###

| **Date**      | **Message**             |
|:--------------|:------------------------|
| 07-Nov-2009   | Version 0.3a released   |
| 11-Nov-2008   | Version 0.2a released   |
| 01-Oct-2008   | Version 0.1a released   |


### Features ###

  * Documents (XML, plain text or binary) are stored in binary files on the file system.

  * Documents are hierarchically ordered in a tree of collections, similar to directories in a file system.

  * XML documents can be automatically validated against their schemas.

  * Documents can be quickly retrieved using indices based on document keys. XML documents can be indexed automatically when inserted or updated based on their contents. Document keys can also be set manually, which is especially useful for non-XML documents.

  * Validation, indices and compression are configured per collection using an inheritance tree.

  * Queries, written in the XQuery language, can be executed ad-hoc or using stored modules.

  * Features a re-entrant read/write locking mechanism at the collection/document level.

  * Interfaces:
    * Java API (embedded library)
    * WebDAV (server) _(planned)_
    * REST (server) _(planned)_

The XQuery functionality is powered by Saxon HE, an open source XQuery and XSLT processor written by Michael H. Kay from Saxonica Limited (http://www.saxonica.com).


### Status ###

Stable (alpha) as embedded Java library. Comes with a decent number of unit tests to guarantee reliability and stability.

Now working on the REST and WebDAV interfaces to extends its use with a role as server (standalone or in a web container).

Please note that much more testing is required for Xantippe to be considered mature enough for production, so use at your own risk!