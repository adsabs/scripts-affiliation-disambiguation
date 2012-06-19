# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""BibRecord - XML MARC processing library for Invenio.

For API, see create_record(), record_get_field_instances() and friends
in the source code of this file in the section entitled INTERFACE.

Note: Does not access the database, the input is MARCXML only."""

### IMPORT INTERESTING MODULES AND XML PARSERS

import re
try:
    import psyco
    PSYCO_AVAILABLE = True
except ImportError:
    PSYCO_AVAILABLE = False

# verbose level to be used when creating records from XML: (0=least, ..., 9=most)
CFG_BIBRECORD_DEFAULT_VERBOSE_LEVEL = 0

# correction level to be used when creating records from XML: (0=no, 1=yes)
CFG_BIBRECORD_DEFAULT_CORRECT = 0

# XML parsers available:
CFG_BIBRECORD_PARSERS_AVAILABLE = ['pyrxp', '4suite', 'minidom']

# Exceptions
class InvenioBibRecordParserError(Exception):
    """A generic parsing exception for all available parsers."""
    pass

class InvenioBibRecordFieldError(Exception):
    """An generic error for BibRecord."""
    pass

# verbose level to be used when creating records from XML: (0=least, ..., 9=most)
## CFG_BIBUPLOAD_EXTERNAL_OAIID_TAG -- where do we store OAI ID tags
## of harvested records?  Useful for matching when we harvest stuff
## via OAI that we do not want to reexport via Invenio OAI; so records
## may have only the source OAI ID stored in this tag (kind of like
## external system number too).
CFG_BIBUPLOAD_EXTERNAL_OAIID_TAG = '035__a'

# Some values used for the RXP parsing.
TAG, ATTRS, CHILDREN = 0, 1, 2

# Find out about the best usable parser:
AVAILABLE_PARSERS = []

# Do we remove singletons (empty tags)?
# NOTE: this is currently set to True as there are some external workflow
# exploiting singletons, e.g. bibupload -c used to delete fields, and
# bibdocfile --fix-marc called on a record where the latest document
# has been deleted.
CFG_BIBRECORD_KEEP_SINGLETONS = True

try:
    import pyRXP
    if 'pyrxp' in CFG_BIBRECORD_PARSERS_AVAILABLE:
        AVAILABLE_PARSERS.append('pyrxp')
except ImportError:
    pass

try:
    import Ft.Xml.Domlette
    if '4suite' in CFG_BIBRECORD_PARSERS_AVAILABLE:
        AVAILABLE_PARSERS.append('4suite')
except ImportError:
    pass
except Exception, err:
    from warnings import warn
    warn("Error when importing 4suite: %s" % err)
    pass

try:
    import xml.dom.minidom
    import xml.parsers.expat
    if 'minidom' in CFG_BIBRECORD_PARSERS_AVAILABLE:
        AVAILABLE_PARSERS.append('minidom')
except ImportError:
    pass

### INTERFACE / VISIBLE FUNCTIONS

def create_field(subfields=None, ind1=' ', ind2=' ', controlfield_value='',
    global_position=-1):
    """
    Returns a field created with the provided elements. Global position is
    set arbitrary to -1."""
    if subfields is None:
        subfields = []

    ind1, ind2 = _wash_indicators(ind1, ind2)
    field = (subfields, ind1, ind2, controlfield_value, global_position)
    _check_field_validity(field)
    return field

def create_records(marcxml, verbose=CFG_BIBRECORD_DEFAULT_VERBOSE_LEVEL,
    correct=CFG_BIBRECORD_DEFAULT_CORRECT, parser='',
    keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a list of records from the marcxml description. Returns a
    list of objects initiated by the function create_record(). Please
    see that function's docstring."""
    # Use the DOTALL flag to include newlines.
    regex = re.compile('<record.*?>.*?</record>', re.DOTALL)
    record_xmls = regex.findall(marcxml)

    return [create_record(record_xml, verbose=verbose, correct=correct,
            parser=parser, keep_singletons=keep_singletons) for record_xml in record_xmls]

def create_record(marcxml, verbose=CFG_BIBRECORD_DEFAULT_VERBOSE_LEVEL,
    correct=CFG_BIBRECORD_DEFAULT_CORRECT, parser='',
    sort_fields_by_indicators=False,
    keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a record object from the marcxml description.

    Uses the best parser available in CFG_BIBRECORD_PARSERS_AVAILABLE or
    the parser specified.

    The returned object is a tuple (record, status_code, list_of_errors),
    where status_code is 0 when there are errors, 1 when no errors.

    The return record structure is as follows:
    Record := {tag : [Field]}
    Field := (Subfields, ind1, ind2, value)
    Subfields := [(code, value)]

    For example:
                                ______
                               |record|
                                ------
        __________________________|_______________________________________
       |record['001']             |record['909']           |record['520'] |
       |                          |                        |              |
[list of fields]             [list of fields]       [list of fields]     ...
       |                    ______|______________          |
       |[0]                |[0]          |[1]    |         |[0]
    ___|_____         _____|___       ___|_____ ...    ____|____
   |Field 001|       |Field 909|     |Field 909|      |Field 520|
    ---------         ---------       ---------        ---------
     |     _______________|_________________    |             |
    ...   |[0]            |[1]    |[2]      |  ...           ...
          |               |       |         |
    [list of subfields]  'C'     '4'
       ___|__________________________________________
       |                    |                        |
('a', 'value') ('b', 'value for subfield b') ('a', 'value for another a')

    @param marcxml: an XML string representation of the record to create
    @param verbose: the level of verbosity: 0 (silent), 1-2 (warnings),
        3(strict:stop when errors)
    @param correct: 1 to enable correction of marcxml syntax. Else 0.
    @return: a tuple (record, status_code, list_of_errors), where status
        code is 0 where there are errors, 1 when no errors"""
    # Select the appropriate parser.
    parser = _select_parser(parser)

    try:
        if parser == 'pyrxp':
            rec = _create_record_rxp(marcxml, verbose, correct,
                keep_singletons=keep_singletons)
        elif parser == '4suite':
            rec = _create_record_4suite(marcxml,
                keep_singletons=keep_singletons)
        elif parser == 'minidom':
            rec = _create_record_minidom(marcxml,
                keep_singletons=keep_singletons)
    except InvenioBibRecordParserError, ex1:
        return (None, 0, str(ex1))

    if sort_fields_by_indicators:
        _record_sort_by_indicators(rec)

    errs = []
    if correct:
        # Correct the structure of the record.
        errs = _correct_record(rec)

    return (rec, int(not errs), errs)

def record_get_field_instances(rec, tag="", ind1=" ", ind2=" "):
    """Returns the list of field instances for the specified tag and
    indicators of the record (rec).

    Returns empty list if not found.
    If tag is empty string, returns all fields

    Parameters (tag, ind1, ind2) can contain wildcard %.

    @param rec: a record structure as returned by create_record()
    @param tag: a 3 characters long string
    @param ind1: a 1 character long string
    @param ind2: a 1 character long string
    @param code: a 1 character long string
    @return: a list of field tuples (Subfields, ind1, ind2, value,
        field_position_global) where subfields is list of (code, value)"""
    if not rec:
        return []
    if not tag:
        return rec.items()
    else:
        out = []
        ind1, ind2 = _wash_indicators(ind1, ind2)

        if '%' in tag:
            # Wildcard in tag. Check all possible
            for field_tag in rec:
                if _tag_matches_pattern(field_tag, tag):
                    for possible_field_instance in rec[field_tag]:
                        if (ind1 in ('%', possible_field_instance[1]) and
                            ind2 in ('%', possible_field_instance[2])):
                            out.append(possible_field_instance)
        else:
            # Completely defined tag. Use dict
            for possible_field_instance in rec.get(tag, []):
                if (ind1 in ('%', possible_field_instance[1]) and
                    ind2 in ('%', possible_field_instance[2])):
                    out.append(possible_field_instance)
        return out

def record_get_field_value(rec, tag, ind1=" ", ind2=" ", code=""):
    """Returns first (string) value that matches specified field
    (tag, ind1, ind2, code) of the record (rec).

    Returns empty string if not found.

    Parameters (tag, ind1, ind2, code) can contain wildcard %.

    Difference between wildcard % and empty '':

    - Empty char specifies that we are not interested in a field which
      has one of the indicator(s)/subfield specified.

    - Wildcard specifies that we are interested in getting the value
      of the field whatever the indicator(s)/subfield is.

    For e.g. consider the following record in MARC:
      100C5  $$a val1
      555AB  $$a val2
      555AB      val3
      555    $$a val4
      555A       val5

      >> record_get_field_value(record, '555', 'A', '', '')
      >> "val5"
      >> record_get_field_value(record, '555', 'A', '%', '')
      >> "val3"
      >> record_get_field_value(record, '555', 'A', '%', '%')
      >> "val2"
      >> record_get_field_value(record, '555', 'A', 'B', '')
      >> "val3"
      >> record_get_field_value(record, '555', '', 'B', 'a')
      >> ""
      >> record_get_field_value(record, '555', '', '', 'a')
      >> "val4"
      >> record_get_field_value(record, '555', '', '', '')
      >> ""
      >> record_get_field_value(record, '%%%', '%', '%', '%')
      >> "val1"

    @param rec: a record structure as returned by create_record()
    @param tag: a 3 characters long string
    @param ind1: a 1 character long string
    @param ind2: a 1 character long string
    @param code: a 1 character long string
    @return: string value (empty if nothing found)"""
    # Note: the code is quite redundant for speed reasons (avoid calling
    # functions or doing tests inside loops)
    ind1, ind2 = _wash_indicators(ind1, ind2)

    if '%' in tag:
        # Wild card in tag. Must find all corresponding fields
        if code == '':
            # Code not specified.
            for field_tag, fields in rec.items():
                if _tag_matches_pattern(field_tag, tag):
                    for field in fields:
                        if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                            # Return matching field value if not empty
                            if field[3]:
                                return field[3]
        elif code == '%':
            # Code is wildcard. Take first subfield of first matching field
            for field_tag, fields in rec.items():
                if _tag_matches_pattern(field_tag, tag):
                    for field in fields:
                        if (ind1 in ('%', field[1]) and ind2 in ('%', field[2])
                            and field[0]):
                            return field[0][0][1]
        else:
            # Code is specified. Take corresponding one
            for field_tag, fields in rec.items():
                if _tag_matches_pattern(field_tag, tag):
                    for field in fields:
                        if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                            for subfield in field[0]:
                                if subfield[0] == code:
                                    return subfield[1]

    else:
        # Tag is completely specified. Use tag as dict key
        if tag in rec:
            if code == '':
                # Code not specified.
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        # Return matching field value if not empty
                        # or return "" empty if not exist.
                        if field[3]:
                            return field[3]

            elif code == '%':
                # Code is wildcard. Take first subfield of first matching field
                for field in rec[tag]:
                    if (ind1 in ('%', field[1]) and ind2 in ('%', field[2]) and
                        field[0]):
                        return field[0][0][1]
            else:
                # Code is specified. Take corresponding one
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        for subfield in field[0]:
                            if subfield[0] == code:
                                return subfield[1]
    # Nothing was found
    return ""

def record_get_field_values(rec, tag, ind1=" ", ind2=" ", code=""):
    """Returns the list of (string) values for the specified field
    (tag, ind1, ind2, code) of the record (rec).

    Returns empty list if not found.

    Parameters (tag, ind1, ind2, code) can contain wildcard %.

    @param rec: a record structure as returned by create_record()
    @param tag: a 3 characters long string
    @param ind1: a 1 character long string
    @param ind2: a 1 character long string
    @param code: a 1 character long string
    @return: a list of strings"""
    tmp = []

    ind1, ind2 = _wash_indicators(ind1, ind2)

    if '%' in tag:
        # Wild card in tag. Must find all corresponding tags and fields
        tags = [k for k in rec if _tag_matches_pattern(k, tag)]
        if code == '':
            # Code not specified. Consider field value (without subfields)
            for tag in tags:
                for field in rec[tag]:
                    if (ind1 in ('%', field[1]) and ind2 in ('%', field[2]) and
                        field[3]):
                        tmp.append(field[3])
        elif code == '%':
            # Code is wildcard. Consider all subfields
            for tag in tags:
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        for subfield in field[0]:
                            tmp.append(subfield[1])
        else:
            # Code is specified. Consider all corresponding subfields
            for tag in tags:
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        for subfield in field[0]:
                            if subfield[0] == code:
                                tmp.append(subfield[1])
    else:
        # Tag is completely specified. Use tag as dict key
        if rec and tag in rec:
            if code == '':
                # Code not specified. Consider field value (without subfields)
                for field in rec[tag]:
                    if (ind1 in ('%', field[1]) and ind2 in ('%', field[2]) and
                        field[3]):
                        tmp.append(field[3])
            elif code == '%':
                # Code is wildcard. Consider all subfields
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        for subfield in field[0]:
                            tmp.append(subfield[1])
            else:
                # Code is specified. Take corresponding one
                for field in rec[tag]:
                    if ind1 in ('%', field[1]) and ind2 in ('%', field[2]):
                        for subfield in field[0]:
                            if subfield[0] == code:
                                tmp.append(subfield[1])

    # If tmp was not set, nothing was found
    return tmp

def field_get_subfield_values(field_instance, code):
    """Return subfield CODE values of the field instance FIELD."""
    return [subfield_value
            for subfield_code, subfield_value in field_instance[0]
            if subfield_code == code]

#### IMPLEMENTATION / INVISIBLE FUNCTIONS

def _check_field_validity(field):
    """
    Checks if a field is well-formed.

    @param field: A field tuple as returned by create_field()
    @type field:  tuple
    @raise InvenioBibRecordFieldError: If the field is invalid.
    """
    if type(field) not in (list, tuple):
        raise InvenioBibRecordFieldError("Field of type '%s' should be either "
            "a list or a tuple." % type(field))

    if len(field) != 5:
        raise InvenioBibRecordFieldError("Field of length '%d' should have 5 "
            "elements." % len(field))

    if type(field[0]) not in (list, tuple):
        raise InvenioBibRecordFieldError("Subfields of type '%s' should be "
            "either a list or a tuple." % type(field[0]))

    if type(field[1]) is not str:
        raise InvenioBibRecordFieldError("Indicator 1 of type '%s' should be "
            "a string." % type(field[1]))

    if type(field[2]) is not str:
        raise InvenioBibRecordFieldError("Indicator 2 of type '%s' should be "
            "a string." % type(field[2]))

    if type(field[3]) is not str:
        raise InvenioBibRecordFieldError("Controlfield value of type '%s' "
            "should be a string." % type(field[3]))

    if type(field[4]) is not int:
        raise InvenioBibRecordFieldError("Global position of type '%s' should "
            "be an int." % type(field[4]))

    for subfield in field[0]:
        if (type(subfield) not in (list, tuple) or
            len(subfield) != 2 or
            type(subfield[0]) is not str or
            type(subfield[1]) is not str):
            raise InvenioBibRecordFieldError("Subfields are malformed. "
                "Should a list of tuples of 2 strings.")

def _tag_matches_pattern(tag, pattern):
    """Returns true if MARC 'tag' matches a 'pattern'.

    'pattern' is plain text, with % as wildcard

    Both parameters must be 3 characters long strings.

    For e.g.
    >> _tag_matches_pattern("909", "909") -> True
    >> _tag_matches_pattern("909", "9%9") -> True
    >> _tag_matches_pattern("909", "9%8") -> False

    @param tag: a 3 characters long string
    @param pattern: a 3 characters long string
    @return: False or True"""
    for char1, char2 in zip(tag, pattern):
        if char2 not in ('%', char1):
            return False
    return True

def _record_sort_by_indicators(record):
    """Sorts the fields inside the record by indicators."""
    for tag, fields in record.items():
        record[tag] = _fields_sort_by_indicators(fields)

def _fields_sort_by_indicators(fields):
    """Sorts a set of fields by their indicators. Returns a sorted list
    with correct global field positions."""
    field_dict = {}
    field_positions_global = []
    for field in fields:
        field_dict.setdefault(field[1:3], []).append(field)
        field_positions_global.append(field[4])

    indicators = field_dict.keys()
    indicators.sort()

    field_list = []
    for indicator in indicators:
        for field in field_dict[indicator]:
            field_list.append(field[:4] + (field_positions_global.pop(0),))

    return field_list

def _select_parser(parser=None):
    """Selects the more relevant parser based on the parsers available
    and on the parser desired by the user."""
    if not AVAILABLE_PARSERS:
        # No parser is available. This is bad.
        return None

    if parser is None or parser not in AVAILABLE_PARSERS:
        # Return the best available parser.
        return AVAILABLE_PARSERS[0]
    else:
        return parser

def _create_record_rxp(marcxml, verbose=CFG_BIBRECORD_DEFAULT_VERBOSE_LEVEL,
    correct=CFG_BIBRECORD_DEFAULT_CORRECT,
    keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a record object using the RXP parser.

    If verbose>3 then the parser will be strict and will stop in case of
    well-formedness errors or DTD errors.
    If verbose=0, the parser will not give warnings.
    If 0 < verbose <= 3, the parser will not give errors, but will warn
    the user about possible mistakes

    correct != 0 -> We will try to correct errors such as missing
    attributes
    correct = 0 -> there will not be any attempt to correct errors"""
    if correct:
        # Note that with pyRXP < 1.13 a memory leak has been found
        # involving DTD parsing. So enable correction only if you have
        # pyRXP 1.13 or greater.
        marcxml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
            '<collection>\n%s\n</collection>' % (marcxml))

    # Create the pyRXP parser.
    pyrxp_parser = pyRXP.Parser(ErrorOnValidityErrors=0, ProcessDTD=1,
        ErrorOnUnquotedAttributeValues=0, srcName='string input')

    if verbose > 3:
        pyrxp_parser.ErrorOnValidityErrors = 1
        pyrxp_parser.ErrorOnUnquotedAttributeValues = 1

    try:
        root = pyrxp_parser.parse(marcxml)
    except pyRXP.error, ex1:
        raise InvenioBibRecordParserError(str(ex1))

    # If record is enclosed in a collection tag, extract it.
    if root[TAG] == 'collection':
        children = _get_children_by_tag_name_rxp(root, 'record')
        if not children:
            return {}
        root = children[0]

    record = {}
    # This is needed because of the record_xml_output function, where we
    # need to know the order of the fields.
    field_position_global = 1

    # Consider the control fields.
    for controlfield in _get_children_by_tag_name_rxp(root, 'controlfield'):
        if controlfield[CHILDREN]:
            value = ''.join([n for n in controlfield[CHILDREN]])
            # Construct the field tuple.
            field = ([], ' ', ' ', value, field_position_global)
            record.setdefault(controlfield[ATTRS]['tag'], []).append(field)
            field_position_global += 1
        elif keep_singletons:
            field = ([], ' ', ' ', '', field_position_global)
            record.setdefault(controlfield[ATTRS]['tag'], []).append(field)
            field_position_global += 1

    # Consider the data fields.
    for datafield in _get_children_by_tag_name_rxp(root, 'datafield'):
        subfields = []
        for subfield in _get_children_by_tag_name_rxp(datafield, 'subfield'):
            if subfield[CHILDREN]:
                value = _get_children_as_string_rxp(subfield[CHILDREN])
                subfields.append((subfield[ATTRS].get('code', '!'), value))
            elif keep_singletons:
                subfields.append((subfield[ATTRS].get('code', '!'), ''))

        if subfields or keep_singletons:
            # Create the field.
            tag = datafield[ATTRS].get('tag', '!')
            ind1 = datafield[ATTRS].get('ind1', '!')
            ind2 = datafield[ATTRS].get('ind2', '!')
            ind1, ind2 = _wash_indicators(ind1, ind2)
            # Construct the field tuple.
            field = (subfields, ind1, ind2, '', field_position_global)
            record.setdefault(tag, []).append(field)

            field_position_global += 1

    return record

def _create_record_from_document(document,
        keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a record from the document (of type
    xml.dom.minidom.Document or Ft.Xml.Domlette.Document)."""
    root = None
    for node in document.childNodes:
        if node.nodeType == node.ELEMENT_NODE:
            root = node
            break

    if root is None:
        return {}

    if root.tagName == 'collection':
        children = _get_children_by_tag_name(root, 'record')
        if not children:
            return {}
        root = children[0]

    field_position_global = 1
    record = {}

    for controlfield in _get_children_by_tag_name(root, "controlfield"):
        tag = controlfield.getAttributeNS(None, "tag").encode('utf-8')

        text_nodes = controlfield.childNodes
        value = ''.join([n.data for n in text_nodes]).encode("utf-8")

        if value or keep_singletons:
            field = ([], " ", " ", value, field_position_global)
            record.setdefault(tag, []).append(field)
            field_position_global += 1

    for datafield in _get_children_by_tag_name(root, "datafield"):
        subfields = []

        for subfield in _get_children_by_tag_name(datafield, "subfield"):
            value = _get_children_as_string(subfield.childNodes).encode("utf-8")
            if value or keep_singletons:
                code = subfield.getAttributeNS(None, 'code').encode("utf-8")
                subfields.append((code or '!', value))

        if subfields or keep_singletons:
            tag = datafield.getAttributeNS(None, "tag").encode("utf-8") or '!'

            ind1 = datafield.getAttributeNS(None, "ind1").encode("utf-8")
            ind2 = datafield.getAttributeNS(None, "ind2").encode("utf-8")
            ind1, ind2 = _wash_indicators(ind1, ind2)
            field = (subfields, ind1, ind2, "", field_position_global)

            record.setdefault(tag, []).append(field)
            field_position_global += 1

    return record

def _create_record_minidom(marcxml,
        keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a record using minidom."""
    try:
        dom = xml.dom.minidom.parseString(marcxml)
    except xml.parsers.expat.ExpatError, ex1:
        raise InvenioBibRecordParserError(str(ex1))

    return _create_record_from_document(dom, keep_singletons=keep_singletons)

def _create_record_4suite(marcxml,
        keep_singletons=CFG_BIBRECORD_KEEP_SINGLETONS):
    """Creates a record using the 4suite parser."""
    try:
        dom = Ft.Xml.Domlette.NonvalidatingReader.parseString(marcxml,
            "urn:dummy")
    except Ft.Xml.ReaderException, ex1:
        raise InvenioBibRecordParserError(ex1.message)

    return _create_record_from_document(dom, keep_singletons=keep_singletons)

def _get_children_by_tag_name(node, name):
    """Retrieves all children from node 'node' with name 'name' and
    returns them as a list."""
    try:
        return [child for child in node.childNodes if child.nodeName == name]
    except TypeError:
        return []

def _get_children_by_tag_name_rxp(node, name):
    """Retrieves all children from 'children' with tag name 'tag' and
    returns them as a list.
    children is a list returned by the RXP parser"""
    try:
        return [child for child in node[CHILDREN] if child[TAG] == name]
    except TypeError:
        return []

def _get_children_as_string(node):
    """
    Iterates through all the children of a node and returns one string
    containing the values from all the text-nodes recursively.
    """
    out = []
    if node:
        for child in node:
            if child.nodeType == child.TEXT_NODE:
                out.append(child.data)
            else:
                out.append(_get_children_as_string(child.childNodes))
    return ''.join(out)

def _get_children_as_string_rxp(node):
    """
    RXP version of _get_children_as_string():

    Iterates through all the children of a node and returns one string
    containing the values from all the text-nodes recursively.
    """
    out = []
    if node:
        for child in node:
            if type(child) is str:
                out.append(child)
            else:
                out.append(_get_children_as_string_rxp(child[CHILDREN]))
    return ''.join(out)

def _wash_indicators(*indicators):
    """
    Washes the values of the indicators. An empty string or an
    underscore is replaced by a blank space.

    @param indicators: a series of indicators to be washed
    @return: a list of washed indicators
    """
    return [indicator in ('', '_') and ' ' or indicator
            for indicator in indicators]

def _correct_record(record):
    """
    Checks and corrects the structure of the record.

    @param record: the record data structure
    @return: a list of errors found
    """
    errors = []

    for tag in record.keys():
        upper_bound = '999'
        n = len(tag)

        if n > 3:
            i = n - 3
            while i > 0:
                upper_bound = '%s%s' % ('0', upper_bound)
                i -= 1

        # Missing tag. Replace it with dummy tag '000'.
        if tag == '!':
            errors.append((1, '(field number(s): ' +
                str([f[4] for f in record[tag]]) + ')'))
            record['000'] = record.pop(tag)
            tag = '000'
        elif not ('001' <= tag <= upper_bound or tag in ('FMT', 'FFT')):
            errors.append(2)
            record['000'] = record.pop(tag)
            tag = '000'

        fields = []
        for field in record[tag]:
            # Datafield without any subfield.
            if field[0] == [] and field[3] == '':
                errors.append((8, '(field number: ' + str(field[4]) + ')'))

            subfields = []
            for subfield in field[0]:
                if subfield[0] == '!':
                    errors.append((3, '(field number: ' + str(field[4]) + ')'))
                    newsub = ('', subfield[1])
                else:
                    newsub = subfield
                subfields.append(newsub)

            if field[1] == '!':
                errors.append((4, '(field number: ' + str(field[4]) + ')'))
                ind1 = " "
            else:
                ind1 = field[1]

            if field[2] == '!':
                errors.append((5, '(field number: ' + str(field[4]) + ')'))
                ind2 = " "
            else:
                ind2 = field[2]

            fields.append((subfields, ind1, ind2, field[3], field[4]))

        record[tag] = fields

    return errors

if PSYCO_AVAILABLE:
    psyco.bind(_correct_record)
    psyco.bind(_create_record_4suite)
    psyco.bind(_create_record_rxp)
    psyco.bind(_create_record_minidom)
    psyco.bind(field_get_subfield_values)
    psyco.bind(create_records)
    psyco.bind(create_record)
    psyco.bind(record_get_field_instances)
    psyco.bind(record_get_field_value)
    psyco.bind(record_get_field_values)
