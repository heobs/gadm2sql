#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Majormode.  All rights reserved.
#
# This software is the confidential and proprietary information of
# Majormode or one of its subsidiaries.  You shall not disclose this
# confidential information and shall use it only in accordance with
# the terms of the license agreement or other applicable agreement you
# entered into with Majormode.
#
# MAJORMODE MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
# SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.  MAJORMODE
# SHALL NOT BE LIABLE FOR ANY LOSSES OR DAMAGES SUFFERED BY LICENSEE
# AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS
# DERIVATIVES.
#
# @version $Revision$


# This script downloads the Global Administrative Areas (GADM) spatial
# database, containing location of the world's administrative areas,
# it extracts data, and generates SQL commands to insert these data
# into a GIS database. The data are available as shapefile,
# Environmental Systems Research Institute (ESRI) geodatabase.
#
# (1) The script downloads the "shapefile" ZIP archive file that
# consists of at least four actual files (.shp, .shx, .dbf, .prj),
# four files per administrative subdivision.
#
#     +----------------------------------------------+
#     | {country_code}_adm{administrative_level}.dbf |
#     +----------------------------------------------+
#     | {country_code}_adm{administrative_level}.shp |
#     +----------------------------------------------+
#     | {country_code}_adm{administrative_level}.shx |
#     +----------------------------------------------+
#     | {country_code}_adm{administrative_level}.prj |
#     +----------------------------------------------+
#     :                                              :
#
# * ``.shp``: shape format; the feature geometry itself
#
# * ``.shx``: shape index format; a positional index of the feature
#   geometry to allow seeking forwards and backwards quickly
#
# * ``.dbf``: attribute format; columnar attributes for each shape, in
#   dBase IV format
#
# The script reads the .shp file to retrieve the boundaries of each
# administrative subdivision of a given administrative level.  The
# coordinate reference system is longitude/latitude (WGS84 datum).
#
# (2) Unfortunately, many of the non standard latin characters are
# lost in the .shp file.  The script uses the .dbf file that comes
# with the shapefiles, but unfortunately this file contains
# unintelligible symbols or numbers.  This issue is encountered when
# the country’s language is not English (or has accents, additional
# characters, etc.), while the dBase IV file extracted from the shape
# archive file used ISO-8859-1, or more likely Windows-1252 (CP-1252),
# which is a character encoding of the Latin alphabet, a superset of
# ISO 8859-1. are a few Unicode.
#
# .. note:: the .csv file that also comes with the shapefiles, has a
#    same issue.
#
# (3) The scripts then to download an additional ZIP archive file: the
# ESRI file geodatabase, which is the standard format used by ArcGIS.
# It consists of a Microsoft Access file, where all the textual
# information is encoded in UTF-8.


# from __future__ import unicode_literals


from majormode.utils import file_util
from majormode.utils import zip_util

import argparse
import chardet
import codecs
import gc
import locale
import os
import re
import shapefile
import string
import subprocess
import sys
import time
import tempfile
import traceback
import unidecode
import uuid
import zipfile


# List of countries available in the database of the Global
# Administrative Areas (GADM).
GADM_SUPPORTED_COUNTRIES = [
    ('AFG', u"Afghanistan"),
    ('ALA', u"Åland"),
    ('ALB', u"Albania"),
    ('DZA', u"Algeria"),
    ('ASM', u"American Samoa"),
    ('AND', u"Andorra"),
    ('AGO', u"Angola"),
    ('AIA', u"Anguilla"),
    ('ATA', u"Antarctica"),
    ('ATG', u"Antigua and Barbuda"),
    ('ARG', u"Argentina"),
    ('ARM', u"Armenia"),
    ('ABW', u"Aruba"),
    ('AUS', u"Australia"),
    ('AUT', u"Austria"),
    ('AZE', u"Azerbaijan"),
    ('BHS', u"Bahamas"),
    ('BHR', u"Bahrain"),
    ('BGD', u"Bangladesh"),
    ('BRB', u"Barbados"),
    ('BLR', u"Belarus"),
    ('BEL', u"Belgium"),
    ('BLZ', u"Belize"),
    ('BEN', u"Benin"),
    ('BMU', u"Bermuda"),
    ('BTN', u"Bhutan"),
    ('BOL', u"Bolivia"),
    ('BES', u"Bonaire, Saint Eustatius and Saba"),
    ('BIH', u"Bosnia and Herzegovina"),
    ('BWA', u"Botswana"),
    ('BVT', u"Bouvet Island"),
    ('BRA', u"Brazil"),
    ('IOT', u"British Indian Ocean Territory"),
    ('VGB', u"British Virgin Islands"),
    ('BRN', u"Brunei"),
    ('BGR', u"Bulgaria"),
    ('BFA', u"Burkina Faso"),
    ('BDI', u"Burundi"),
    ('KHM', u"Cambodia"),
    ('CMR', u"Cameroon"),
    ('CAN', u"Canada"),
    ('CPV', u"Cape Verde"),
    ('XCA', u"Caspian Sea"),
    ('CYM', u"Cayman Islands"),
    ('CAF', u"Central African Republic"),
    ('TCD', u"Chad"),
    ('CHL', u"Chile"),
    ('CHN', u"China"),
    ('CXR', u"Christmas Island"),
    ('XCL', u"Clipperton Island"),
    ('CCK', u"Cocos Islands"),
    ('COL', u"Colombia"),
    ('COM', u"Comoros"),
    ('COK', u"Cook Islands"),
    ('CRI', u"Costa Rica"),
    ('CIV', u"Côte d'Ivoire"),
    ('HRV', u"Croatia"),
    ('CUB', u"Cuba"),
    ('CUW', u"Curaçao"),
    ('CYP', u"Cyprus"),
    ('CZE', u"Czech Republic"),
    ('COD', u"Democratic Republic of the Congo"),
    ('DNK', u"Denmark"),
    ('DJI', u"Djibouti"),
    ('DMA', u"Dominica"),
    ('DOM', u"Dominican Republic"),
    ('TLS', u"East Timor"),
    ('ECU', u"Ecuador"),
    ('EGY', u"Egypt"),
    ('SLV', u"El Salvador"),
    ('GNQ', u"Equatorial Guinea"),
    ('ERI', u"Eritrea"),
    ('EST', u"Estonia"),
    ('ETH', u"Ethiopia"),
    ('FLK', u"Falkland Islands"),
    ('FRO', u"Faroe Islands"),
    ('FJI', u"Fiji"),
    ('FIN', u"Finland"),
    ('FRA', u"France"),
    ('GUF', u"French Guiana"),
    ('PYF', u"French Polynesia"),
    ('ATF', u"French Southern Territories"),
    ('GAB', u"Gabon"),
    ('GMB', u"Gambia"),
    ('GEO', u"Georgia"),
    ('DEU', u"Germany"),
    ('GHA', u"Ghana"),
    ('GIB', u"Gibraltar"),
    ('GRC', u"Greece"),
    ('GRL', u"Greenland"),
    ('GRD', u"Grenada"),
    ('GLP', u"Guadeloupe"),
    ('GUM', u"Guam"),
    ('GTM', u"Guatemala"),
    ('GGY', u"Guernsey"),
    ('GIN', u"Guinea"),
    ('GNB', u"Guinea-Bissau"),
    ('GUY', u"Guyana"),
    ('HTI', u"Haiti"),
    ('HMD', u"Heard Island and McDonald Islands"),
    ('HND', u"Honduras"),
    ('HKG', u"Hong Kong"),
    ('HUN', u"Hungary"),
    ('ISL', u"Iceland"),
    ('IND', u"India"),
    ('IDN', u"Indonesia"),
    ('IRN', u"Iran"),
    ('IRQ', u"Iraq"),
    ('IRL', u"Ireland"),
    ('IMN', u"Isle of Man"),
    ('ISR', u"Israel"),
    ('ITA', u"Italy"),
    ('JAM', u"Jamaica"),
    ('JPN', u"Japan"),
    ('JEY', u"Jersey"),
    ('JOR', u"Jordan"),
    ('KAZ', u"Kazakhstan"),
    ('KEN', u"Kenya"),
    ('KIR', u"Kiribati"),
    ('XKO', u"Kosovo"),
    ('KWT', u"Kuwait"),
    ('KGZ', u"Kyrgyzstan"),
    ('LAO', u"Laos"),
    ('LVA', u"Latvia"),
    ('LBN', u"Lebanon"),
    ('LSO', u"Lesotho"),
    ('LBR', u"Liberia"),
    ('LBY', u"Libya"),
    ('LIE', u"Liechtenstein"),
    ('LTU', u"Lithuania"),
    ('LUX', u"Luxembourg"),
    ('MAC', u"Macao"),
    ('MKD', u"Macedonia"),
    ('MDG', u"Madagascar"),
    ('MWI', u"Malawi"),
    ('MYS', u"Malaysia"),
    ('MDV', u"Maldives"),
    ('MLI', u"Mali"),
    ('MLT', u"Malta"),
    ('MHL', u"Marshall Islands"),
    ('MTQ', u"Martinique"),
    ('MRT', u"Mauritania"),
    ('MUS', u"Mauritius"),
    ('MYT', u"Mayotte"),
    ('MEX', u"Mexico"),
    ('FSM', u"Micronesia"),
    ('MDA', u"Moldova"),
    ('MCO', u"Monaco"),
    ('MNG', u"Mongolia"),
    ('MNE', u"Montenegro"),
    ('MSR', u"Montserrat"),
    ('MAR', u"Morocco"),
    ('MOZ', u"Mozambique"),
    ('MMR', u"Myanmar"),
    ('NAM', u"Namibia"),
    ('NRU', u"Nauru"),
    ('NPL', u"Nepal"),
    ('NLD', u"Netherlands"),
    ('NCL', u"New Caledonia"),
    ('NZL', u"New Zealand"),
    ('NIC', u"Nicaragua"),
    ('NER', u"Niger"),
    ('NGA', u"Nigeria"),
    ('NIU', u"Niue"),
    ('NFK', u"Norfolk Island"),
    ('PRK', u"North Korea"),
    ('MNP', u"Northern Mariana Islands"),
    ('NOR', u"Norway"),
    ('OMN', u"Oman"),
    ('PAK', u"Pakistan"),
    ('PLW', u"Palau"),
    ('PSE', u"Palestina"),
    ('PAN', u"Panama"),
    ('PNG', u"Papua New Guinea"),
    ('PRY', u"Paraguay"),
    ('PER', u"Peru"),
    ('PHL', u"Philippines"),
    ('PCN', u"Pitcairn Islands"),
    ('POL', u"Poland"),
    ('PRT', u"Portugal"),
    ('PRI', u"Puerto Rico"),
    ('QAT', u"Qatar"),
    ('COG', u"Republic of Congo"),
    ('REU', u"Reunion"),
    ('ROU', u"Romania"),
    ('RUS', u"Russia"),
    ('RWA', u"Rwanda"),
    ('SHN', u"Saint Helena"),
    ('KNA', u"Saint Kitts and Nevis"),
    ('LCA', u"Saint Lucia"),
    ('SPM', u"Saint Pierre and Miquelon"),
    ('VCT', u"Saint Vincent and the Grenadines"),
    ('BLM', u"Saint-Barthélemy"),
    ('MAF', u"Saint-Martin"),
    ('WSM', u"Samoa"),
    ('SMR', u"San Marino"),
    ('STP', u"Sao Tome and Principe"),
    ('SAU', u"Saudi Arabia"),
    ('SEN', u"Senegal"),
    ('SRB', u"Serbia"),
    ('SYC', u"Seychelles"),
    ('SLE', u"Sierra Leone"),
    ('SGP', u"Singapore"),
    ('SXM', u"Sint Maarten"),
    ('SVK', u"Slovakia"),
    ('SVN', u"Slovenia"),
    ('SLB', u"Solomon Islands"),
    ('SOM', u"Somalia"),
    ('ZAF', u"South Africa"),
    ('SGS', u"South Georgia and the South Sandwich Islands"),
    ('KOR', u"South Korea"),
    ('SSD', u"South Sudan"),
    ('ESP', u"Spain"),
    # ('SP-', u"Spratly islands"), Not supported anymore?
    ('LKA', u"Sri Lanka"),
    ('SDN', u"Sudan"),
    ('SUR', u"Suriname"),
    ('SJM', u"Svalbard and Jan Mayen"),
    ('SWZ', u"Swaziland"),
    ('SWE', u"Sweden"),
    ('CHE', u"Switzerland"),
    ('SYR', u"Syria"),
    ('TWN', u"Taiwan"),
    ('TJK', u"Tajikistan"),
    ('TZA', u"Tanzania"),
    ('THA', u"Thailand"),
    ('TGO', u"Togo"),
    ('TKL', u"Tokelau"),
    ('TON', u"Tonga"),
    ('TTO', u"Trinidad and Tobago"),
    ('TUN', u"Tunisia"),
    ('TUR', u"Turkey"),
    ('TKM', u"Turkmenistan"),
    ('TCA', u"Turks and Caicos Islands"),
    ('TUV', u"Tuvalu"),
    ('UGA', u"Uganda"),
    ('UKR', u"Ukraine"),
    ('ARE', u"United Arab Emirates"),
    ('GBR', u"United Kingdom"),
    ('USA', u"United States"),
    ('UMI', u"United States Minor Outlying Islands"),
    ('URY', u"Uruguay"),
    ('UZB', u"Uzbekistan"),
    ('VUT', u"Vanuatu"),
    ('VAT', u"Vatican City"),
    ('VEN', u"Venezuela"),
    ('VNM', u"Vietnam"),
    ('VIR', u"Virgin Islands"),
    ('WLF', u"Wallis and Futuna"),
    ('ESH', u"Western Sahara"),
    ('YEM', u"Yemen"),
    ('ZMB', u"Zambia"),
    ('ZWE', u"Zimbabwe"),
]

# Maximum time in seconds that the Global Administrative Areas (GADM)
# ZIP archives stored in the local cache are allowed to be reused.
# When this time expires, the script needs to fetch data from the
# origin server.
GADM_CACHE_EXPIRATION_TIME = 60 * 60 * 24 * 7

# Template of Uniform Resource Locator (URL) that references the ZIP
# archive file of the ESRI geodatabase of a country provided by the
# Global Administrative Areas (GADM) project.
GADM_ESRI_ARCHIVE_URL_TEMPLATE = 'http://biogeo.ucdavis.edu/data/gadm2.8/mdb/%s_adm_mdb.zip'

# Template of Uniform Resource Locator (URL) that references the ZIP
# archive file of shapefile of a country provided by the Global
# Administrative Areas (GADM) project.
GADM_SHAPEFILE_ARCHIVE_URL_TEMPLATE = 'http://biogeo.ucdavis.edu/data/gadm2.8/shp/%s_adm_shp.zip'

# Character to request ``mdb-export`` to use to separate columns while
# exported data of a given ESRI geodatabase table into a CVS-like
# output.
MDB_COLUMN_DELIMITER = '@'

# Character to request ``mdb-export`` to use to separate rows while
# exported data of a given ESRI geodatabase table into a CVS-like
# output.
MDB_ROW_DELIMITER = '$'


# Python determines the encoding of stdout and stderr based on the
# value of the ``LC_CTYPE`` variable, but only if the stdout is a tty.
# So if you just output to the terminal, ``LC_CTYPE`` (or ``LC_ALL``)
# define the encoding.  However, when the output is piped to a file or
# to a different process, the encoding is not defined, and defaults to
# 7-bit ASCII, which raise the following exception when outputting
# unicode characters:
#
#   ``UnicodeEncodeError: 'ascii' codec can't encode character u'\x..' in position 18: ordinal not in range(128).``
#
# @note: using the environment variable ``PYTHONIOENCODING`` is
#     another solution, however it requires the user to prefix the
#     command line with ``PYTHONIOENCODING=utf-8``, which is more
#     cumbersome.
sys.stdout = codecs.getwriter(sys.stdout.encoding if sys.stdout.isatty() \
        else locale.getpreferredencoding())(sys.stdout)


class AdministrativeSubdivision(object):
    """
    Represent an administrative subdivision extracted from the database of
    Global Administrative Areas (GADM).  An administrative subdivision is
    a demarcated geographic area of the Earth, such as a portion of a
    country or other region delineated for the purpose of administration.

    Countries are divided up into these smaller units.  For example, a
    country may be divided into provinces, which, in turn, are divided
    into counties, which, in turn, may be divided in whole or in part into
    municipalities; and so on.
    """
    # Define the list of characters allowed to build the code used to
    # reference an administrative division in the GADM database.
    CODE_ALLOWED_CHARACTERS = string.digits + '.'

    def __init__(self, code, name, level, area_type, boundaries):
        """
        Build a ``AdministrativeSubdivision`` instance which data are
        retrieved from a GADM shape file (boundaries of this administrative
        subdivision) and a GADM dBase IV file (name and additional meta data).

        The name of the administrative subdivision is initially retrieved from
        the dBase IV file extracted from the shape archive file.  The original
        DBF standard defines to use ISO-8859-1, or more likely Windows-1252
        (CP-1252), which is a character encoding of the Latin alphabet, a
        superset of ISO 8859-1.

        @note: some columns contain unintelligible symbols or numbers, that’s
            due to unavailable or non-western fonts.  This issue will be
            encountered if the country’s language is not English (or has
            accents, additional characters, etc.).  The columns with weird
            characters contain the variants of the location names.


        @param code: code used to reference this area in the GADM database.
            It corresponds to a dot-separated list of codes of the parents in
            reversed hierarchical order: ``[parent.code]*.code``.

        @param name: name of the administative subdivision.

        @param level: administrative level of this area.  For clarity and
            convenience the standard neutral reference for the largest
            administrative subdivision of a country is called the "first-level
            administrative division" or "first administrative level".  Next
            smaller is called "second-level administrative division" or "second
            administrative level".

        @param area_type: type of this subdivision, if known.  There is no
            naming convention as each country might have its own
            administrative division classification,

        @param boundaries: a list of tuples, each of them corresponding to the
            boundary of one geographical area of the administrative
            subdivision.  A boundary is a list of tuples of geographical
            coordinates ``(longitude, latitude)`` that define a closed line
            (the perimeter) of this area.

                [
                  ((lon1, lat1), (lon2, lat2), ... , (lon1, lat1)), # area 1
                  ...
                ]
        """
        self.id = uuid.uuid4()
        self.code = AdministrativeSubdivision.cleanse_subdivision_code(code)

        # [PATCH:20150615] It appears sometimes that GADM doesn't contain the
        # data of the direct parent of an administrative subdivision.  The
        # script will fix this issue by linking the administrative subdivision
        # to a grandparent, updating then accordingly the parent code of this
        # subdivision.
        self.parent_code = '.'.join([ subcode for subcode in code.split('.')[:-1] ])

        # [PATCH:20150615] It appears that sometimes names are not given in
        # English, but a local language, while the dBase IV file extracted
        # from the shape archive file cannot extended non-latin characters,
        # which results in encoding issue.  We need to convert them to ASCII
        # characters; this name will be overrided later on when parsing the
        # ESRI geodatabase, a Microsoft Access file, that supports UTF-8.
        try:
            probable_encoding = chardet.detect(name)
            self.name = unidecode.unidecode(name.decode(probable_encoding['encoding'] or 'cp1251'))
        except UnicodeDecodeError: # 'charmap' codec can't decode byte 0x8f in position 1: character maps to <undefined>
            self.name = u''.join([ c for c in name if ord(c) < 128 ])

        # [PATCH:20170312] ERROR: literal carriage return found in data
        self.name = self.name.replace('\n', '').replace('\r', '')

        self.level = level

        # [PATCH:20150615] It appears that sometimes types are not given in
        # English, but a local language, while the dBase IV file extracted
        # from the shape archive file cannot extended non-latin characters,
        # which results in encoding issue.  We need to convert them to ASCII
        # characters; this name will be overrided later on when parsing the
        # ESRI geodatabase, a Microsoft Access file, that supports UTF-8.
        self.area_type = unidecode.unidecode(unicode(area_type, 'cp1252')) if area_type else \
            ('country' if level == 0 else None)

        self.boundaries = boundaries

        # [PATCH:20160302] Check whether the coordinate values of the boundaries
        # of this administrative subdivision are in the range [-180 -90, 180 90].
        invalid_coordinates = [ (longitude, latitude) for boundary in boundaries
                for (longitude, latitude) in boundary
                    if not (-180.0 <= longitude <= 180) and (-90 <= latitude <= 90) ]
        if invalid_coordinates:
            print '[WARNING] Invalid coordinates of subdivision %s:' % self.name, invalid_coordinates

    @staticmethod
    def cleanse_subdivision_code(code):
        """
        Cleanse the code of an administrative subdivision. It happens
        sometimes that one of the sub-ids of an administrative division
        contains unexpected dot-character.

        @param code: an administrative subdivision's code, which should
            correspond to a dot-separated list of codes of the parents in
            reversed hierarchical order: ``[parent.code]*.code``.

        @return: a dot-separated list of codes, cleansed for any extra
            characters.
        """
        cleansed_code = ''.join([ c for c in code if c in AdministrativeSubdivision.CODE_ALLOWED_CHARACTERS ])
        return '.'.join([ subcode for subcode in cleansed_code.split('.') if len(subcode) > 0 ])


def build_administrative_subdivisions(zip_file, country_code, administrative_level_count):
    """
    Download the ZIP archive of the shape files of the administrative
    subdivisions of the specified country, uncompress it into memory, and
    retrieve the geographical boundaries, from the first-level -- which is
    the largest administrative subdivision of a country -- to the smallest
    administrative level, which depends on this country.

    The Database of Global Administrative Areas (GADM) provides a ZIP
    archive which contains the following mandatory files:

    * (country_code)_adm(administrative_level).shp: the geographical
      boundaries of all the administrative subdivision of this country of
      a particular administrative level.

    * (country_code)_adm(administrative_level).dbf: columnar attributes, in
      dBase IV format for each administrative subdivision shape of this
      country of a particular administrative level.

    This ZIP archive may contain additional file that this function
    doesn't uses.


    @param country_code: an ISO 3166-1 alpha-2 code representing the
        country to retrieve is administrative subdivision shapes.


    @return: a dictionary of ``AdministrativeSubdivision`` instances of
        all the administrative subdivisions of this country, whatever
        their administrative level.  The key corresponds to the code of
        an administrative subdivision, while the value is the instance
        itself.

    @raise Exception: if a network issue occurs.
    """
    administrative_subdivisions = {}

    for administrative_level in range(administrative_level_count):
        print '[INFO] Parsing administrative level %d...' % administrative_level
        shape_memory_mapped_file = zip_util.open_entry_file(zip_file, '%s_adm%d.shp' % (country_code, administrative_level))
        dbase_memory_mapped_file = zip_util.open_entry_file(zip_file, '%s_adm%d.dbf' % (country_code, administrative_level))
        reader = shapefile.Reader(shp=shape_memory_mapped_file, dbf=dbase_memory_mapped_file)

        field_names = [ field[0] for field in reader.fields[1:] ]

        for shapeRecord in reader.shapeRecords():
            attributes = dict(zip(field_names, shapeRecord.record))

            geometry = shapeRecord.shape.__geo_interface__
            geomerty_type = geometry['type']
            if geomerty_type != 'Polygon' and geomerty_type != 'MultiPolygon':
                raise Exception('Unexpected geometry type "%s"' % geomerty_type)

            geomerty_coordinates = geometry['coordinates']

            administrative_subdivision = AdministrativeSubdivision(
                    '.'.join([ str(attributes['ID_%d' % i]) for i in range(administrative_level + 1) ]),
                     attributes['NAME_ENGLI'].strip() if administrative_level == 0 \
                            else attributes['NAME_%d' % administrative_level].strip(), # VARNAME_%d not always exists
                     administrative_level,
                     'Country' if administrative_level == 0 else attributes['ENGTYPE_%d' % administrative_level].strip(),
                     [ geomerty_coordinates[0] ] if geomerty_type == 'Polygon' else [ geometry[0] for geometry in geomerty_coordinates ])

            # [PATCH:20150615] Check this subdivision has a parent, and if not,
            # try to link this subdivision to a grand-parent.
            if administrative_subdivision.level > 0:
                subcodes = administrative_subdivision.code.split('.')
                parent_subdivision_code = '.'.join([ subcode for subcode in subcodes[:-1] ])
                if not administrative_subdivisions.get(parent_subdivision_code):
                    while True:
                        subcodes = subcodes[:-1]
                        parent_subdivision_code = '.'.join([ subcode for subcode in subcodes[:-1] ])
                        parent_subdivision = administrative_subdivisions.get(parent_subdivision_code)
                        if parent_subdivision:
                            sys.stderr.write('[WARNING] %s (%s-%s) does have a direct parent; fix with grand parent %s\n' % \
                                    (administrative_subdivision.name, country_code, administrative_subdivision.code, parent_subdivision.code))
                            administrative_subdivision.parent_code = parent_subdivision_code
                            break

                        else:
                            if len(subcodes) == 1:
                                sys.stderr.write('[ERROR] The administrative subdivision %s (%s-%s) CANNOT be linked to a parent!\n' % \
                                    (administrative_subdivision.name, country_code, administrative_subdivision.code))
                                break

            administrative_subdivisions[administrative_subdivision.code] = administrative_subdivision

    return administrative_subdivisions


def extract_esri_files(zip_file, country_code, administrative_level_count):
    """
    Providing a ZIP archive file of a Environmental Systems Research
    Institute (ESRI) geodatabase, this function returns a list of files
    where it has stored CSV-formatted data extracted from the Microsoft
    Access database for every administrative subdivision of the given
    country.  There is one file per administrative subdivision level.

    @param zip_file: a ESRI ZIP archive instance.

    @param country_code: a ISO 3166-1 alpha-2 code referencing the
        country of the administrative subdivisions stored in the given
        ESRI geodatabase.

    @param administrative_level_count: the number of administrative
        subdivision levels defined for this country


    @return: a list of absolute path and name of files containing the CSV
        data of each administrative subdivision of the given country.
        These files are sorted by ascending order of their administrative
        subdivision level (the data of country level have been stripped
        from this list as they do not contain any interested information).


    @note: the caller is responsible for deleting the files returned by
        this function.
    """
    # Extract the ESRI archive in a temporary file that this function will
    # delete later.
    mdb_file_entry_name = '%s_adm.mdb' % country_code
    (fd, mdb_file_path_name) = tempfile.mkstemp()
    with os.fdopen(fd, 'wb') as file_handle:
        file_handle.write(zip_file.read(mdb_file_entry_name))

    # Export each administrative subdivision data in the MDB database
    # table to a CSV-formatted file.
    file_path_names = []

    for administrative_level in range(1, administrative_level_count): # Skip country level (nothing interesting to retrieve)
        command_line = 'mdb-export -Q -d %s -R %s -b strip %s %s' % \
                (MDB_COLUMN_DELIMITER,
                 MDB_ROW_DELIMITER,
                 mdb_file_path_name,
                 '%s_adm%d' % (country_code.replace('-', '_'), administrative_level))

        (fd, file_path_name) = tempfile.mkstemp()
        file_path_names.append(file_path_name)
        with os.fdopen(fd, 'wt') as file_handle:
            file_handle.write(subprocess.check_output(command_line, shell=True))

    # Delete the temporary file.
    os.remove(mdb_file_path_name)

    return file_path_names


def fetch_archive_file(archive_url,
        archive_file_name=None,
        memory_mapped=False,
        cache_path=None,
        cache_required=False,
        cache_expiration_time=GADM_CACHE_EXPIRATION_TIME):
    """
    Fetch a GADM ZIP archive referenced by the specified Uniform Resource
    Locator (URL) from either the local cache, either the GADM server.


    @param archive_url: Uniform Resource Locator (URL) referencing the
        GADM ZIP archive to fetch.

    @param url: Uniform Resource Locator that specifies the location of
        the ZIP archive file on a computer network and a mechanism for
        retrieving it.

    @param archive_file_name: The name of the file in which the archive
        will be stored in.  If not defined, the function downloads the
        archive file in a temporary file created in the most secure
        manner possible; the user is responsible for deleting this
        temporary file when done with it.

    @param memory_mapped: indicate whether to map the ZIP archive file-
        like object into memory, for performance optimization, or whether
        to store this archive into disk, for memory optimization.
        If the archive is directly stored into disk, the caller is
        responsible for deleting the temporary file when done with it.

    @param cache_path: absolute path of the cache where downloaded files
        are stored in.

    @param cache_required: indicate whether the store the downloaded files
        in the local cache, or whether to generate temporary files that
        the caller is responsible for deleting.

    @param cache_expiration_time: time in seconds during which the cached
        representation of the GADM data is considered fresh.  Fresh data
        are served directly from the cache, otherwise they are fetched
        from the origin server.


    @return: a tuple ``(file_path_name, zip_file)`` where:

    * ``file_path_name``: the absolute path and name of the file where
      the ZIP archive has been downloaded and stored in.

    * ``zip_file``: a ``ZipFile`` instance of the remote ZIP archive
      file.
    """
    zip_file = None

    if cache_required:
        assert cache_path, 'The GADM cache path is not defined'

        if not os.path.exists(cache_path):
            file_util.make_directory_if_not_exists(cache_path)

        zip_file_path_name = os.path.join(cache_path, archive_file_name)
        if os.path.exists(zip_file_path_name) and time.time() - os.stat(zip_file_path_name).st_mtime < cache_expiration_time:
            zip_file = zipfile.ZipFile(zip_file_path_name)

    if not zip_file:
        print '[INFO] Download %s' % archive_url
        (zip_file, zip_file_path_name) = zip_util.download_archive_file(archive_url,
                file_path_name=zip_file_path_name if cache_required else None,
                memory_mapped=memory_mapped,
                verbose=False)

    return zip_file, zip_file_path_name


def fetch_country_data(country_code,
        cache_path=None,
        cache_required=False,
        cache_expiration_time=GADM_CACHE_EXPIRATION_TIME):
    """
    Fetch shape and ESRI ZIP archive files from either the locale cache,
    either GADM Web site.  Extract shape and ESRI files of the available
    administrative subdivisions of the specified country.


    @param country_code: an ISO 3166-1 alpha-2 code representing the
        country to retrieve its administrative subdivision data.

    @param cache_path: absolute path of the cache where downloaded files
        are stored in.

    @param cache_required: indicate whether the store the downloaded files
        in the local cache, or whether to generate temporary files that
        the caller is responsible for deleting.

    @param cache_expiration_time: time in seconds during which the cached
        representation of the GADM data is considered fresh.  Fresh data
        are served directly from the cache, otherwise they are fetched
        from the origin server.


    @return: a tuple containing the following member in this order:

        * ``country_code``: an ISO 3166-1 alpha-2 code representing the
            country which the administrative subdivision data are
            returned.

        * ``administrative_level_count``: the number of administrative
          subdivisions available for this country.

        * ``shape_zip_file_path_name``: the absolute path and name of the
          shape ZIP archive file.

        * ``esri_zip_file_path_name``: the absolute path and name of the
          ESRI geodatabase ZIP archive file.

        * ``esri_file_path_names``: a list of absolute path and name of
          files containing the CSV data of each administrative subdivision
          of the given country.  These files are sorted by ascending order
          of their administrative subdivision level (the data of country
          level have been stripped from this list as they do not contain
          any interested information).


    @note: the caller is responsible for deleting the files returned by
        this function, when the caller didn't want to cache them.
    """
    # Fetch the shape and the ESRI archives, either from the local cache,
    # either directly form the GADM Web site.
    shape_zip_file, shape_zip_file_path_name = fetch_archive_file(GADM_SHAPEFILE_ARCHIVE_URL_TEMPLATE % country_code,
            archive_file_name='%s_gadm.dbf.zip' % country_code,
            cache_path=cache_path,
            cache_required=cache_required,
            cache_expiration_time=cache_expiration_time,
            memory_mapped=False)

    # Determine the number of available administrative subdivisions for
    # this country.  It corresponds to the number of available entries in
    # the ZIP file.
    administrative_level_count = 0
    while True:
        try:
            zip_util.open_entry_file(shape_zip_file, '%s_adm%d.shp' % (country_code, administrative_level_count))
            administrative_level_count += 1
        except KeyError: # No smaller administrative subdivision
            break

    # Extract the CSV files of the data extracted from the ESRI
    # geodatabase for every administrative subdivision of the given
    # country.  There is one file per administrative subdivision level.
    esri_zip_file, esri_zip_file_path_name = fetch_archive_file(GADM_ESRI_ARCHIVE_URL_TEMPLATE % country_code,
            archive_file_name='%s_gadm.mdb.zip' % country_code,
            cache_path=cache_path,
            cache_required=cache_required,
            cache_expiration_time=cache_expiration_time,
            memory_mapped=False)

    esri_file_path_names = extract_esri_files(esri_zip_file, country_code, administrative_level_count)

    shape_zip_file.close()
    esri_zip_file.close()

    return country_code,\
           administrative_level_count, \
           shape_zip_file_path_name, \
           esri_zip_file_path_name, \
           esri_file_path_names


def update_administrative_subdivision_metadata(country_code, administrative_subdivisions, esri_file_path_names):
    """
    Update the metadata of a country's administrative subdivisions,
    overriding those already defined.

    This operation is useful as the first metadata were extracted from the
    shape file of this country, which sometimes contain unintelligible
    symbols or numbers.  This issue is encountered when the country’s
    language is not English (or has accents, additional characters, etc.),
    while the dBase IV file extracted from the shape archive file used
    ISO-8859-1, or more likely Windows-1252 (CP-1252), which is a
    character encoding of the Latin alphabet, a superset of ISO 8859-1.

    This function retrieve the ESRI geodatabase of this country, a
    Microsoft Access file, where all the textual information is encoded in
    UTF-8.

    The function overrides the ``AdministrativeSubdivision`` instances
    that are contained in the dictionary object passed to this function.


    @param country_code: an ISO 3166-1 alpha-2 code representing the
        country to update metadata.

    @param administrative_subdivisions: a dictionary of
        ``AdministrativeSubdivision`` instances of all the administrative
        subdivisions of this country.  The key corresponds to the code of
        an administrative subdivision, while the value is the instance
        itself.


    @return: the dictionary of administrative subdivisions that was passed
        to this function.
    """
    for administrative_level in range(1, len(esri_file_path_names)): # Country level has been stripped out.
        with open(esri_file_path_names[administrative_level - 1], 'rt') as fileobj:
            data = fileobj.read()

        # [PATCH:20150614] It appears sometimes that GADM contains a few
        #  administrative subdivision which name has a carriage return
        # character.
        data = data.replace('\n', '').replace(r'\n', '').replace('\r', '').replace(r'\r', '')

        rows = data.split(MDB_ROW_DELIMITER)

        header_fields = rows[0].split(MDB_COLUMN_DELIMITER)

        for record in rows[1:-1]: # Skip the header row and the last empty row.
            attributes = dict([ (header_fields[i], attribute_value)
                    for (i, attribute_value) in enumerate(record.split(MDB_COLUMN_DELIMITER)) ])

            subdivision_code = AdministrativeSubdivision.cleanse_subdivision_code(
                    '.'.join([ str(attributes['ID_%d' % i]) for i in range(administrative_level + 1) ]))
            subdivision_name = attributes['NAME_%d' % administrative_level]
            subdivision_type = attributes.get('TYPE_%d' % administrative_level) or attributes.get('ENGTYPE_%d' % administrative_level)

            try:
                administrative_subdivision = administrative_subdivisions[subdivision_code]
                administrative_subdivision.name = unicode(subdivision_name, 'utf-8').strip().replace('\n', '').replace('\r', '')

                administrative_subdivision.area_type = unicode(subdivision_type, 'utf-8').strip() if subdivision_type else None

            except KeyError:
                sys.stderr.write('[WARNING] %s (%s-%s) is referenced in MDB but not in SHP; ignore it.\n' % \
                        (unicode(subdivision_name, 'utf-8').strip(),
                         country_code,
                         subdivision_code))

    return administrative_subdivisions


def write_sql_commands(administrative_subdivisions,
            sql_file_path_name=None,
            country_code=None):
    """
    Write on the standard output the list of `COPY commands
    <http://www.postgresql.org/docs/current/static/sql-copy.html>`_ to
    populate the tables ``area`` and ``area_label`` with the data of the
    specified administrative subdivisions.


    @note: these commands are supported by PostgreSQL 9.x+.  There is no
        COPY statement in the SQL standard.

    @note: all the characters written to the standard output are encoded
        in UTF-8.


    @param administrative_subdivisions: a dictionary of
        ``AdministrativeSubdivision`` instances of all the administrative
        subdivisions of this country.  The key corresponds to the code of
        an administrative subdivision, while the value is the instance
        itself.

    @param sql_file_path_name: absolute path and name of the file where
        the SQL commands needs to be written in.
    """
    with file_util.smart_open(sql_file_path_name, 'a') as file_handle:
        # Write the administrative subdivisions' information, including their
        # boundaries.
        print >>file_handle, 'COPY area(area_id, parent_area_id, area_code, area_type, area_level, boundaries) FROM stdin;'

        for subdivision in administrative_subdivisions.itervalues():
            print >>file_handle, \
                    """%(area_id)s\t%(parent_area_id)s\t%(area_code)s\t%(area_type)s\t%(area_level)s\t%(boundaries)s""" % {
                            'area_id': subdivision.id,
                            'area_code': subdivision.code,
                            'area_type': r'\N' if not subdivision.area_type else subdivision.area_type,
                            'area_level': subdivision.level,
                            'boundaries': r'SRID=4326;MULTIPOLYGON(%s)' % (
                                    (','.join([ '((%s))' % (','.join([ '%s %s' % (longitude, latitude)
                                            for (longitude, latitude) in boundary ]))
                                        for boundary in subdivision.boundaries ]))),
                            'parent_area_id': r'\N' if subdivision.level == 0 \
                                else administrative_subdivisions[subdivision.parent_code].id }

        # @note: the following list comprehension is probably a more pythonic
        #     approach, but with massive data, it would lead to a ``MemoryError``:
        #
        # print >>file_handle, '\n'.join([
        #         """%(area_id)s\t%(parent_area_id)s\t%(area_code)s\t%(area_type)s\t%(area_level)s\t%(boundaries)s""" % {
        #                 'area_id': subdivision.id,
        #                 'area_code': subdivision.code,
        #                 'area_type': r'\N' if not subdivision.area_type else subdivision.area_type,
        #                 'area_level': subdivision.level,
        #                 'boundaries': r'SRID=4326;MULTIPOLYGON(%s)' % (
        #                         (','.join([ '((%s))' % (','.join([ '%s %s' % (longitude, latitude)
        #                                 for (longitude, latitude) in boundary ]))
        #                             for boundary in subdivision.boundaries ]))),
        #                 'parent_area_id': r'\N' if subdivision.level == 0 \
        #                     else administrative_subdivisions[subdivision.parent_code].id }
        #         for subdivision in administrative_subdivisions.itervalues() ])

        print >>file_handle, r'\.'
        print >>file_handle

        # Write the administrative subdivisions' names.
        print >>file_handle, 'COPY area_label(area_id, content) FROM stdin;'

        print >>file_handle, '\n'.join([
                    """%(area_id)s\t%(content)s""" % {
                            'area_id': subdivision.id,
                            'content': subdivision.name }
                for subdivision in administrative_subdivisions.itervalues() ])
        print >>file_handle, r'\.'
        print >>file_handle

        # Generate the list of keywords of at least 2 characters composing the
        # name of each administrative subdivision.
        print >> file_handle, "\echo 'Indexing administrative subdivisions of country %s'" % country_code

        print >>file_handle, 'COPY area_index(area_id, keyword) FROM stdin;'

        for subdivision in administrative_subdivisions.itervalues():
            subdivision_ascii_name = unidecode.unidecode(subdivision.name).lower()
            punctuationless = re.sub(r"""[.,\/#!$%\^&\*;:{}=\-_`~()<>"']""", ' ', subdivision_ascii_name)
            keywords = re.sub(r'\s{2,}', ' ', punctuationless).split(' ')

            for keyword in keywords:
                if len(keyword) > 1 and not keyword.isdigit():
                    print >>file_handle, """%(area_id)s\t%(keyword)s""" % {
                            'area_id': subdivision.id,
                            'keyword': keyword }

        print >>file_handle, r'\.'
        print >>file_handle

        # print >>file_handle, """
        #         DO $$
        #         BEGIN
        #           IF EXISTS(
        #             SELECT true
        #               FROM pg_proc
        #               WHERE proname = '_simplify_area_boundaries') THEN
        #             RAISE NOTICE 'Simplifying boundaries of each geographic area...';
        #             PERFORM _simplify_area_boundaries();
        #           ELSE
        #             RAISE NOTICE 'No function found to simplify boundaries of each geographic area.';
        #           END IF;
        #         END $$;"""
        # print >>file_handle


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest='sql_file_path_name', metavar='filename', required=True,
            help='write the SQL commands to the specified file %(metavar)s')
    parser.add_argument('--cache-path', metavar='cache-path',
            help='specify the absolute path where the ZIP archives downloaded from GADM server need to be cached')
    parser.add_argument('--cache', dest='cache_required', action='store_true',
            help='indicate whether the ZIP archives downloaded from GADM server need to be cached on the local disk')
    parser.add_argument('--cache-expiration_time', type=int, default=GADM_CACHE_EXPIRATION_TIME, metavar='expiration-time',
            help='specify the maximum time in seconds that ZIP archives downloaded from'
                 'GADM server and stored in the local cache are allowed to be reused.'
                 'When this time expires, the script needs to fetch data from the origin'
                 'server.')
    arguments = parser.parse_args()

    # Sanity test to check that the command line executable mdb-export is
    # installed on the computer this script is running on.
    if file_util.which('mdb-export') is None:
        raise Exception('The mdb-export executable has not been found while this program is required to use this script')

    # Preprocess the countries, fetching archive ZIP files from either the
    # locale cache, either GADM Web site; extract shape and ESRI files
    # of the available administrative subdivisions per country.
    #
    # @note: the script extracts all the required files first, instead of
    # processing each country, and extracting the corresponding files
    # only, since the latter method raised a non-obvious error when
    # executing the Shell command mdb-export to extract CSV data from the
    # ESRI geodatabase:
    #
    #        File "/usr/lib/python2.7/subprocess.py", line xxxx, in _execute_child
    #        self.pid = os.fork()
    #    OSError: [Errno 12] Cannot allocate memory
    #
    countries_data = []
    for (country_code, country_name) in GADM_SUPPORTED_COUNTRIES:
        print '[INFO] Fetching %s data...' % country_name
        countries_data.append(fetch_country_data(country_code,
                cache_path=(arguments.cache_path or os.path.join(os.path.expanduser('~'), '.gadm') if arguments.cache_required else None),
                cache_required=arguments.cache_required,
                cache_expiration_time=arguments.cache_expiration_time))

    # Check whether the SQL commands need to be written into a file or to
    # the standard output.  In the first case, this script overrides any
    # existing file.
    sql_file_path_name = None if arguments.sql_file_path_name == '-' else arguments.sql_file_path_name

    if sql_file_path_name and os.path.exists(sql_file_path_name):
        os.remove(sql_file_path_name)

    # Retrieve the shapes and the names of the administrative subdivisions
    # of each country, and output the SQL commands to insert data into a
    # database.
    for (country_code, administrative_level_count, shape_zip_file_path_name, esri_zip_file_path_name,  esri_file_path_names) in countries_data:
        print '[INFO] Processing country %s...' % country_code

        with zipfile.ZipFile(shape_zip_file_path_name) as shape_zip_file:
            administrative_subdivisions = build_administrative_subdivisions(shape_zip_file, country_code, administrative_level_count)
            update_administrative_subdivision_metadata(country_code, administrative_subdivisions, esri_file_path_names)
            write_sql_commands(administrative_subdivisions, sql_file_path_name=sql_file_path_name, country_code=country_code)

        # Delete the archive files, if no caching is required, and all the
        # administrative subdivision CSV temporary files extracted from the
        # ESRI geodatabase.
        if not arguments.cache_required:
            os.remove(shape_zip_file_path_name)
            os.remove(esri_zip_file_path_name)

        for esri_file_path_name in esri_file_path_names:
            os.remove(esri_file_path_name)

        gc.collect()

    # Write the PL/pgSQL command to generate the simplified boundaries of
    # the geographical areas.
    with file_util.smart_open(sql_file_path_name, 'a') as file_handle:
        print >> file_handle, """
                DO $$
                BEGIN
                  IF EXISTS(
                    SELECT true
                      FROM pg_proc
                      WHERE proname = '_simplify_area_boundaries') THEN
                    RAISE NOTICE 'Simplifying boundaries of each geographic area...';

                    DROP INDEX idx_area__boundaries;

                    PERFORM _simplify_area_boundaries();

                    -- ALTER TABLE area
                    --  ALTER COLUMN _boundaries SET NOT NULL;

                    CREATE INDEX idx_area__boundaries
                      ON area USING GIST (_boundaries);
                  ELSE
                    RAISE NOTICE 'No function found to simplify boundaries of each geographic area.';
                  END IF;
                END $$;
                """
        print >> file_handle
