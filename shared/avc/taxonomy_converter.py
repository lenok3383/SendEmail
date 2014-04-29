#!/usr/bin/python

"""AVC taxonomy converter API for AVC Rule Checker,
AVC Update Daemon and AVC-NBAR merging System.

This utility is responsible for conversion of AVC taxonomy file
from version 1.1 to version 1.0.

:Status: $Id: //prod/main/_is/shared/python/avc/taxonomy_converter.py#2 $
:Authors: ivlesnik
:Last Modified By: $Author: ivlesnik $
:$Date: 2013/10/17 $
"""

import logging
import os
import re

from avc_common.constants import TaxonomyConst as c

# Global constants.
EXIT_OKAY = 0
EXIT_ERROR = 1
REQUIRED_OPTIONS = (c.MNEMONIC_KW, c.ENTRY_TYPE_KW, c.NAME_KW, c.PARENT_KW)
WSA_MODE = 1
PRE_FALCON_MODE = 2
MODES = {
    WSA_MODE: 'WSA',
    PRE_FALCON_MODE: 'Pre-Falcon',
}
GENERIC_BEHAVIOR_ID = 200
SUPPORTED_VERSIONS = ('1.0', None)


class AVCTaxonomyConverterError(Exception):

    """Generic AVC Taxonomy Converter error class."""

    pass


class AVCTaxonomyConverter(object):

    """AVC Taxonomy Converter class.

    The AVCTaxonomyConverter converts taxonomy file version 1.1 to
    taxonomy file version 1.0.
    """

    def __init__(self):
        """Constuctor for the AVC Taxonomy Converter.

        :Parameters:
            - None
        """

        self._log = logging.getLogger(self.__class__.__name__)
        self._removed_sections = []

    @staticmethod
    def _is_leaf_node(taxonomy, node):
        """Traverses the taxonomy entities tree and checks whether the given
        entity is the leaf node.

        :Parameters:
            - `taxonomy`: taxonomy data dictionary.
            - `node`: taxonomy entity id (section id).

        :Return:
            - Boolean value, True if the given entity is a leaf node,
              or False if it has children.
        """

        for section in taxonomy:
            if (node != section and
                    taxonomy[section][c.ENTRY_TYPE_KW] == c.APP_KW and
                    int(taxonomy[section][c.PARENT_KW]) == int(node)):
                return False
        return True

    def _process_exceptions(self, taxonomy, exceptions, mnemonics):
        """Update app-to-app_type relations according to data in `exceptions`.

        :Parameters:
            - `taxonomy`: taxonomy data dictionary.
            - `exceptions`: dictionary in format {app: app-type, ...}
            - `mnemonics`: dictionary of format {app_type: mnemonic}

        :Return:
            - taxonomy data dictionary.
        """
        for app, app_type in exceptions.iteritems():
            if not app_type in mnemonics:
                self._log.warn('Section with mnemonic=%s' \
                    ' does not exist in taxonomy file', app_type)
                continue

            for section in taxonomy:
                if (taxonomy[section][c.ENTRY_TYPE_KW] == c.APP_KW and
                        taxonomy[section][c.MNEMONIC_KW] == app):
                    apptype_entry = mnemonics[app_type]

                    # Set app_type option for application entry.
                    taxonomy[section][c.APP_TYPE_KW] = apptype_entry

                    # Remove parent option for application entry.
                    if c.PARENT_KW in taxonomy[section]:
                        taxonomy[section].pop(c.PARENT_KW)

                    # Set entry_type option for app_type entry.
                    taxonomy[apptype_entry][c.ENTRY_TYPE_KW] = c.TYPE_KW

                    # Remove parent option for app_type entry.
                    if c.PARENT_KW in taxonomy[apptype_entry]:
                        taxonomy[apptype_entry].pop(c.PARENT_KW)

                    # No need to process the rest sections.
                    break

        return taxonomy

    def _process_wsa_mode(self, taxonomy):
        """Process AVC Taxonomy in WSA mode.

        :Parameters:
            - `taxonomy`: taxonomy data dictionary.
        """

        for section in taxonomy.keys():
            if section in self._removed_sections:
                continue

            # Remove 'generic' behavior section.
            if section == GENERIC_BEHAVIOR_ID:
                del taxonomy[section]
                self._removed_sections.append(section)
                self._log.debug('Section [%s] removed.', section)
                continue

            # Remove generic behavior reference in applications.
            if taxonomy[section][c.ENTRY_TYPE_KW] == c.APP_KW:
                if c.BEHAVIORS_KW in taxonomy[section]:
                    behaviors = taxonomy[section][c.BEHAVIORS_KW].split()
                    if str(GENERIC_BEHAVIOR_ID) in behaviors:
                        behaviors.remove(str(GENERIC_BEHAVIOR_ID))
                        if behaviors:
                            taxonomy[section][c.BEHAVIORS_KW] = \
                                             ' '.join(behaviors)
                    # If 'behaviors' option is empty in taxonomy 1.1 or
                    # it became empty after removing 'generic' from it,
                    # then we remove this option from taxonomy 1.0
                    if not behaviors:
                        del taxonomy[section][c.BEHAVIORS_KW]

            for option in taxonomy[section].keys():
                if option not in c.TAXONOMY_1_0_OPTIONS:
                    taxonomy[section].pop(option)
                    self._log.debug('Option "%s" is not supported by taxonomy '\
                        'version 1.0. Removed from section [%s]',
                        option, section)

        return taxonomy

    def convert(self, taxonomy, to_version='1.0', mode=WSA_MODE,
                exceptions=None):
        """Convert entries from 1.1 to 1.0.

        :Parameters:
            - `taxonomy`: taxonomy data dictionary.
            - `mode`: WSA or Pre-Falcon mode. Optional, defaults to WSA.
            - `exceptions`: dictionary in format {app: app-type, ...}
        :Return:
            - taxonomy dict in format:
                {section_id: {option: value, ...}, ...}
        """

        if to_version not in SUPPORTED_VERSIONS:
            raise AVCTaxonomyConverterError('Cannot convert to unsupported '
                                            'version "%s"' % (to_version,))

        mnemonics = {}
        leaf_to_parent = []

        # Check for required options. If some of them are missing, then
        # log error and raise an exception.
        errors = 0
        for section in taxonomy:
            for option in REQUIRED_OPTIONS:
                try:
                    if (option == c.PARENT_KW and
                            c.ENTRY_TYPE_KW in taxonomy[section] and
                            taxonomy[section][c.ENTRY_TYPE_KW] != c.APP_KW):
                        continue
                    _ = taxonomy[section][option]
                except KeyError:
                    self._log.error('Option "%s" does not exist in the '\
                        'input taxonomy section [%s]', option, section)
                    errors += 1

        if errors > 0:
            raise AVCTaxonomyConverterError(
                '%d error(s) detected in the input taxonomy' % (errors,))

        # Find leaf applications and their direct parents.
        for section in taxonomy:
            if taxonomy[section][c.ENTRY_TYPE_KW] == c.APP_KW:
                mnemonics[taxonomy[section][c.MNEMONIC_KW]] = section
                parent = int(taxonomy[section][c.PARENT_KW])
                if self._is_leaf_node(taxonomy, section):
                    leaf_to_parent.append((section, parent))

        # Convert parents of leaf nodes to application types
        # and set app_type attribute to leaf nodes.
        for section, parent in leaf_to_parent:
            if parent != 0:
                taxonomy[section][c.APP_TYPE_KW] = parent
                taxonomy[section].pop(c.PARENT_KW)
                taxonomy[parent][c.ENTRY_TYPE_KW] = c.TYPE_KW
                if c.PARENT_KW in taxonomy[parent]:
                    taxonomy[parent].pop(c.PARENT_KW)
                if c.BEHAVIORS_KW in taxonomy[parent]:
                    taxonomy[parent].pop(c.BEHAVIORS_KW)

        # Go through exceptions file and set app to app type relations
        # accordingly.
        if exceptions:
            taxonomy = self._process_exceptions(taxonomy, exceptions, mnemonics)

        # Remove all 'app' and 'app_type' entries that were not updated on
        # the previous steps (actually those with existing 'parent' option).
        for section in taxonomy.keys():
            if (taxonomy[section][c.ENTRY_TYPE_KW] == c.APP_KW
                    and c.APP_TYPE_KW not in taxonomy[section]):
                del taxonomy[section]
                self._removed_sections.append(section)
                self._log.debug('Section [%s] removed', section)

        # Handle WSA mode processing.
        if mode == WSA_MODE:
            taxonomy = self._process_wsa_mode(taxonomy)

        return taxonomy
