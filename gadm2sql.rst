==================================
Global Administrative Areas to SQL
==================================

The Database of Global Administrative Areas (GADM) is spatial database
of the location of the world's administrative areas (or adminstrative
boundaries) for use in Geographic Information System (GIS).

Administrative areas in this database are countries and lower level
subdivisions such as provinces, departments, regions, cities, and so
on.  GADM describes where these administrative areas are (the
"spatial features"), and for each area it provides some attributes,
such as the name and variant names.

``gadm2sql`` is a Python script that downloads and parses GADM's data,
and writes to a specified file, or the standard output, a list of
`COPY commands <http://www.postgresql.org/docs/current/static/sql-copy.html>`_
supported by PostgreSQL (there is no COPY statement in the SQL
standard).


------------
Installation
------------

Requirements::

* ``libtool``: computer programming tool from the GNU build system
  used for creating portable compiled libraries::

    sudo apt-get install build-essential libtool

* ``autoconf``: tool that produces configure scripts for building,
  installing and packaging software on computer systems where a
  Bourne shell is available::

    sudo apt-get install autoconf

* ``pkg-config``: tool that provides a unified interface for
  querying installed libraries for the purpose of compiling software
  from its source code::

    sudo apt-get install pkg-config

* ``libglib``: GLib is a library containing many useful C routines
  for things such as trees, hashes, lists, and strings. It is a
  useful general-purpose C library used by projects such as GTK+,
  GIMP, and GNOME::

    sudo apt-get install libglib2.0-dev

* ``txt2man``: program that converts simple texts to manpages
  easily.  If you are not interested in manuals of ``mdbtools``, the
  installation of ``txt2man`` is not required, but in that case, you
  need to configure ``mdbtools`` with ``--disable-man``.  Or simply
  install it::

    sudo apt-get install txt2man


The installation procedure::

    sudo su - devops
    cd bin
    wget -O mdbtools.0.7.1.tar.gz https://github.com/brianb/mdbtools/archive/0.7.1.tar.gz
    tar xvfz mdbtools.0.7.1.tar.gz
    cd mdbtools-0.7.1
    autoreconf -i -f
    ./configure --disable-man
    make
    cd ..
    ln -s mdbtools-0.7.1/src/util/mdb-export

Make sure that your local ``bin`` directory is included in the
``PATH`` environment variable::

    cat ~/.bashrc | grep "~/bin"

should display something like::

    export PATH=~/bin:/usr/local/bin:/opt/bin:$PATH


---------
Execution
---------

Simply execute the following command::


    ./gadm2sql.py - | psql -f - (database) (username)

