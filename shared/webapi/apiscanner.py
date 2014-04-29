"""API scanner tool to convert class instance interface to HTTP URL
representation.

:Status: $Id: //prod/main/_is/shared/python/webapi/apiscanner.py#43 $
:Authors: vburenin, rbodnarc, ober
"""

import cjson
import inspect
import logging
import os
import paste.request
import pprint
import re
import types
import urllib
from collections import deque

from shared.util.encoding import rencode

import errors
from callmapper import *
from validate import (NoneChecker,
                      StringTypeFormat,
                      ASCIIStringTypeFormat,
                      IntTypeFormat)

MODULE_NAME = 'Web Service Meta Data'
FORMAT = {'json': JSON_FORMAT,
          'text': TEXT_FORMAT,
          'rencode': RENCODE_FORMAT}
DEFAULT_FORMAT = 'text'

VERSION_PATTERN = re.compile('^v[\d]+$')

class ServiceExposer(object):
    """Main handler for requests to Web API."""

    def __init__(self, path, default_version=None):
        """ServiceExposer __init__ method.

        :param path: Path to folder with resources.
        :param default_version: Name of the version to be used as
                                the default version.
        """
        self.__log = logging

        self.__interface = {'GET': dict(),
                            'DELETE': dict(),
                            'PUT': dict(),
                            'POST': dict()}
        self.__meta_interface = dict()
        self.__versions = list()

        self.path = path
        self.__default_version = default_version

        self.__help = HelpGen()

        self.__extend_interface()

    def __extend_interface(self):
        """Base helper for autogeneration of API mapping data."""
        resources_map = self.__gen_urlmap()

        if not resources_map:
            raise errors.WebAPIException('No resources found.')

        self.__versions = list(set(resources_map.keys()))

        if not self.__default_version:
            self.__default_version = self.__versions[-1]
        elif self.__default_version not in self.__versions:
            raise errors.WebAPIException('Default version not found.')

        self.__extend_base_resources(resources_map)

        for version, ver_resources in resources_map.iteritems():
            self.__log.info('Process resources from %s version' %
                            (version or 'base',))
            for res in ver_resources:
                self.__check_consistency(res)
                self.__extend_map_data(res, version)
                self.__extend_api_meta_data(res, version)
                self.__extend_help_data(res, version)

    def __extend_base_resources(self, resources_map):
        """Helper for extending mapping with base resources.

        :param resources_map: Dictionary with base resources mapping.
        """
        resources_map[''] = [{'NAME': 'help_info',
                              'HTTP_METHOD': 'GET',
                              'HTTP_URL': '/help_info',
                              'DESCRIPTION': 'Web API help',
                              'URL_PARAMS': [],
                              'QUERY_PARAMS': [['version',
                                        ASCIIStringTypeFormat(none_is_ok=True),
                                        self.__default_version]],
                              'FUNCTION': self.get_help},

                             {'NAME': 'api_meta_data',
                              'HTTP_METHOD': 'GET',
                              'HTTP_URL': '/api_meta_data',
                              'DESCRIPTION': 'Interface Metadata',
                              'URL_PARAMS': [],
                              'QUERY_PARAMS': [['version',
                                        ASCIIStringTypeFormat(none_is_ok=True),
                                        self.__default_version]],
                              'FUNCTION': self.api_meta_data},

                             {'NAME': 'versions',
                              'HTTP_METHOD': 'GET',
                              'HTTP_URL': '/versions',
                              'DESCRIPTION': 'Available resources versions.',
                              'URL_PARAMS': [],
                              'QUERY_PARAMS': [],
                              'FUNCTION': self.versions},

                             {'NAME': 'spec',
                              'HTTP_METHOD': 'GET',
                              'HTTP_URL': '/spec',
                              'DESCRIPTION': 'Web API help in RST format',
                              'URL_PARAMS': [],
                              'QUERY_PARAMS': [['version',
                                        ASCIIStringTypeFormat(none_is_ok=True),
                                        self.__default_version]],
                              'FUNCTION': self.get_spec}]

    def __check_consistency(self, resource):
        """Helper for checking consistency for resource mapping.

        :param resource: Resource mapping to be checked.
        """
        if not isinstance(resource, dict):
            raise errors.InconsistencyMappingError(
                    'Resource mapping should be an instance'
                    ' of \'dict\' type: %s'
                    % (resource,))

        for key in ['NAME', 'FUNCTION']:
            if not key in resource:
                raise errors.InconsistencyMappingError(
                        'No required mapping key \'%s\'. Mapping:\n%s'
                        % (key, resource,))

        module_name = inspect.getmodule(resource['FUNCTION']).__name__
        resource_name = resource['NAME']
        err_location = 'module \'%s\', resource \'%s\'' % (module_name,
                                                           resource_name)

        # Check consistency in mapping structure
        self.__check_map_strucutre(resource, err_location)

        # Check consistency in mapping values type and structure
        self.__check_map_values_structure(resource, err_location)

        # Check parameters names, default values and possibility to be None
        self.__check_map_params_values(resource, err_location)

    def __check_map_strucutre(self, resource, err_location):
        """Helper for checking structure of mapping dictionary.

        :param resource: Dictionary with mapping information.
        :param err_location: String with module name and resource name
                             to be added to error description.
        """
        needed_mapping_keys = ['HTTP_METHOD', 'HTTP_URL', 'DESCRIPTION',
                               'URL_PARAMS', 'QUERY_PARAMS']

        for key in needed_mapping_keys:
            if not key in resource:
                raise errors.InconsistencyMappingError(
                        'No required mapping key %s in %s'
                        % (key, err_location))

    def __assert_right_type(self, resource, keys, needed_type, err_location):
        """Helper for checking type of mapping dictionary values.

        :param resource: Dictionary with mapping information.
        :param keys: Keys of values to be checked.
        :param needed_type: Expected type of values.
        :param err_location: String with module name and resource name
                             to be added to error description.
        """
        for key in keys:
            if not isinstance(resource[key], needed_type):
                raise errors.InconsistencyMappingError(
                        'Invalid type \'%s\' for value \'%s\' in %s.'
                        '  Should be an instance of \'%s\'.'
                        % (type(resource[key]).__name__, resource[key],
                           err_location, needed_type.__name__))

    def __check_map_values_structure(self, resource, err_location):
        """Helper for checking structure of mapping dictionary values.

        :param resource: Dictionary with mapping information.
        :param err_location: String with module name and resource name
                             to be added to error description.
        """
        # Check string values
        self.__assert_right_type(resource, ('NAME', 'HTTP_METHOD',
                                            'HTTP_URL', 'DESCRIPTION'),
                                 str, err_location)

        # Check list values
        self.__assert_right_type(resource, ('URL_PARAMS', 'QUERY_PARAMS'),
                                 list, err_location)

        # Check whether 'HTTP_METHOD' is in valid range
        if resource['HTTP_METHOD'] not in self.__interface:
            raise errors.InconsistencyMappingError(
                    'Invalid \'HTTP_METHOD\' value \'%s\' in %s'
                    % (resource['HTTP_METHOD'], err_location))

        # Check mapping for empty strings
        if not resource['NAME']:
            raise errors.InconsistencyMappingError(
                    'Invalid \'NAME\' value \'%s\' in %s.'
                    '  Value can\'t be empty.'
                    % (resource['NAME'], err_location))

        if resource['HTTP_URL'].startswith('/'):
            if not resource['HTTP_URL'][1:]:
                raise errors.InconsistencyMappingError(
                    'Invalid \'HTTP_URL\' value \'%s\' in %s.'
                    '  Value must contain more than one \'/\'.'
                     % (resource['HTTP_URL'], err_location))
        else:
            raise errors.InconsistencyMappingError(
                    'Invalid \'HTTP_URL\' value \'%s\' in %s.'
                    '  Value must starts with \'/\'.'
                    % (resource['HTTP_URL'], err_location))

        if not callable(resource['FUNCTION']):
            raise errors.InconsistencyMappingError(
                        'Invalid \'FUNCTION\' value %s in %s.  Should be'
                        ' a callable object.'
                        % (resource['FUNCTION'], err_location))

        # Check optional 'LDAP_GROUPS' value
        if 'LDAP_GROUPS' in resource:
            if isinstance(resource['LDAP_GROUPS'], list):
                for group in resource['LDAP_GROUPS']:
                    if not isinstance(group, str):
                        raise errors.InconsistencyMappingError(
                                'Invalid type \'%s\' for value \'%s\' in'
                                ' \'LDAP_GROUPS\' value in %s.  Should be'
                                ' an instance of string.'
                                % (type(group).__name__, group, err_location))

            elif not isinstance(resource['LDAP_GROUPS'], str):
                raise errors.InconsistencyMappingError(
                        'Invalid type \'%s\' for value \'%s\' in %s.  Should'
                        ' be an instance of string or list of strings.'
                        % (type(resource['LDAP_GROUPS']).__name__,
                           resource['LDAP_GROUPS'], err_location))

        # Check structure of parameters mapping
        self.__check_map_params_structure(resource, err_location)

    def __check_map_params_structure(self, resource, err_location):
        """Helper for checking structure of parameters values.

        :param resource: Dictionary with mapping information.
        :param err_location: String with module name and resource name
                             to be added to error description.
        """
        if resource['URL_PARAMS']:
            for param in resource['URL_PARAMS']:
                if not isinstance(param, list) or len(param) != 2 \
                    or not isinstance(param[0], str) \
                    or not isinstance(param[1], (StringTypeFormat,
                                                 IntTypeFormat)):
                    raise errors.InconsistencyMappingError(
                            'Invalid format %s for \'URL_PARAMS\' value in %s.'
                            '  Should be the lists with string at the first'
                            ' position and validator instance at the second.'
                            % (resource['URL_PARAMS'], err_location))

        if resource['QUERY_PARAMS']:
            for param in resource['QUERY_PARAMS']:
                if not (isinstance(param, list) and len(param) in (2, 3) \
                        and isinstance(param[0], str) \
                        and isinstance(param[1], NoneChecker)):
                    raise errors.InconsistencyMappingError(
                            'Invalid format for \'QUERY_PARAMS\' value \'%s\''
                            ' in %s.  Should be the lists with string'
                            ' at the first position, validator instance'
                            ' at the second and  optional default value'
                            ' at the third.'
                            % (resource['QUERY_PARAMS'], err_location))

    def __check_map_params_values(self, resource, err_location):
        """Helper for checking mapping parameters consistency.

        Function checks parameters names, default values, possibility
        to be None and consistency with parameters from function definition.

        :param resource: Dictionary with mapping information.
        :param err_location: String with module name and resource name
                             to be added to error description.
        """
        for param in resource['URL_PARAMS']:
            if param[1].is_none_ok():
                raise errors.InconsistencyMappingError(
                        'Invalid validator for URL parameter \'%s\' in %s.'
                        '  none_is_ok should be False.'
                        % (param[0], err_location))

        param_names = [param[0] for param in resource['URL_PARAMS']]
        param_names.extend([param[0] for param in resource['QUERY_PARAMS']])

        if '' in param_names:
            raise errors.InconsistencyMappingError(
                    'Empty parameter name in %s.'
                    % (err_location,))

        if len(param_names) > len(set(param_names)):
            raise errors.InconsistencyMappingError(
                    'Duplicate parameter name in %s.'
                    % (err_location,))

        # Check mapping for prohibited parameter names
        prohibited_param_names = ('webapi_data_format', USER_OBJECT)
        for name in prohibited_param_names:
            if name in param_names:
                raise errors.InconsistencyMappingError(
                        'Prohibited parameter name \'%s\' in %s.'
                        % (name, err_location))

        self.__check_map_function_params(resource, err_location, param_names)

    def __check_map_function_params(self, resource, err_location, param_names):
        """Helper for checking consistency between mapping and function.

        This checking handles object methods, but in case of using decorators
        please make up some wrappers.

        :param resource: Dictionary with mapping information.
        :param err_location: String with module name and resource name
                             to be added to error description.
        :param param_names: List with mapping parameter names.
        """
        (args, varargs, varkw, defaults) = inspect.getargspec(
                                                resource['FUNCTION'])

        # We won't analyze 'self' parameter
        if inspect.ismethod(resource['FUNCTION']):
            args = args[1:]

        # We won't analyze USER_OBJECT parameter
        if USER_OBJECT in args:
            args.remove(USER_OBJECT)

        if not (varargs and varkw):
            for name in param_names:
                if name not in args:
                    raise errors.InconsistencyMappingError(
                            'Unexpected parameter \'%s\' in %s.'
                            % (name, err_location))

        query_params = [[param[0], param[1].is_none_ok(), param[2:]] \
                        for param in resource['QUERY_PARAMS']]

        if defaults:
            args_without_defaults = args[:-len(defaults)]
        else:
            args_without_defaults = args

        missing_args = set(args_without_defaults).difference(set(param_names))
        if missing_args:
            raise errors.InconsistencyMappingError(
                'Missing mapping description for parameter(s) \'%s\' in %s.'
                % ("', '".join(missing_args), err_location))

        for param in query_params:
            if not param[1] and param[2] and param[2][0] is None:
                raise errors.InconsistencyMappingError(
                        'Prohibited combination none_is_ok=False and'
                        ' def_value=None for parameter \'%s\' in %s.'
                        % (param[0], err_location))

            if param[0] in args_without_defaults \
                and param[1] and not param[2]:
                raise errors.InconsistencyMappingError(
                        'Prohibited combination: no default value in function'
                        ' definition, none_is_ok=True and no default value'
                        ' in mapping data for parameter \'%s\' in %s.'
                        % (param[0], err_location))

    def __extend_map_data(self, resource, version):
        """Helper for extending main interface.

        :param resource: Dictionary with resource mapping.
        :param version: Version of resource.
        """
        if version:
            resource['HTTP_URL'] = '/%s%s' % (version, resource['HTTP_URL'])
            version_prefix = '/%s/' % (version,)
        else:
            version_prefix = '/'

        # Check whether we already have such a function name
        exist_resources = []
        for res_dict in self.__interface.values():
            exist_resources.extend(res_dict.values())

        func_name = resource['NAME']
        exist_functions = (exs_res['NAME'] for exs_res in exist_resources
                           if exs_res['HTTP_URL'].startswith(version_prefix))
        if func_name in exist_functions:
            raise errors.WebAPIException('Duplicate function name found '
                                         'for %s' % (func_name,))

        # Check whether we already have such a resource
        res_id = '%s:%s' % (resource['HTTP_URL'], len(resource['URL_PARAMS']))
        exist_resources = self.__interface[resource['HTTP_METHOD']]
        if res_id in exist_resources:
            raise errors.WebAPIException('Duplicate resource found for '
                                         '%s:%s' %
                                         (resource['HTTP_METHOD'], res_id))
        exist_resources[res_id] = resource

    def __extend_api_meta_data(self, resource, version):
        """Helper for extending meta interface.

        :param resource: Dictionary with resource mapping.
        :param version: Resource version.
        """
        version_amd = self.__meta_interface.setdefault(version, dict())
        amd = {'HTTP_METHOD': resource['HTTP_METHOD'],
               'URL_PARAMS': [param[0] for param in resource['URL_PARAMS']],
               'QUERY_PARAMS': [],
               'HTTP_URL': resource['HTTP_URL']}

        for param in resource['QUERY_PARAMS']:
            name = param[0]
            validator = param[1]
            has_def_value = len(param) > 2

            if not validator.is_none_ok() and not has_def_value:
                is_mandatory = True
            else:
                is_mandatory = False
            amd['QUERY_PARAMS'].append([name, is_mandatory])

        version_amd[resource['NAME']] = amd

    def __extend_help_data(self, resource, version):
        """Helper for extending help interface.

        :param resource: Dictionary with resource mapping.
        :param version: Version of resource.
        """
        self.__help.extend_help(resource, version)

    def __gen_urlmap(self):
        """Helper for scanning resources folder path and accumulating the
        mapping data.

        :return: Dictionary with resources mapping.
        """
        resources_map = dict()

        for name in os.listdir(self.path):
            if os.path.isdir('%s/%s' % (self.path, name)):
                if VERSION_PATTERN.match(name):
                    version_maps = self.__get_version_maps(name)
                    if version_maps:
                        resources_map[name] = version_maps

        return resources_map

    def __get_version_maps(self, version):
        """Scan folder for the specific version, extract 'map_data' variables
        from modules and add this data to ServiceExposer instance.

        :param version: Version/folder name.
        :return: Mapping data for all version's resources.
        """

        files = os.listdir('%s/%s' % (self.path, version))
        files = [f.rsplit('.', 1) for f in files]
        files = [f[0] for f in files if len(f) == 2 and f[1] == 'py']

        version_maps = list()

        for module_file in files:
            # Transform folder path into Python path
            module_path = '%s.%s' % (version, module_file)
            module = __import__(module_path, globals(),
                                locals(), ['map_data'])

            if hasattr(module, 'map_data'):
                version_maps.extend([map_item.copy() for map_item
                                     in getattr(module, 'map_data')])
                self.__log.info('Loaded resources from [%s] module.',
                                module_path)
        return version_maps

    def versions(self):
        """Return dictionary with information about available versions.

        :return: Dictionary with of list of available versions
                 and the default version.
        """
        return {'versions': [v for v in self.__versions if v],
                'default_version': self.__default_version}

    def api_meta_data(self, version):
        """Return API Meta data for client side module.

        :param version: Version of API.
        :return: List of dictionary with metadata and version of API.
        """
        if version not in self.__versions:
            raise errors.NotFoundError('Version %s not found' % (version,))
        return [self.__meta_interface[version], version]

    def get_help(self, version):
        """Return help information.

        :param version: Version of API.
        :return: List of dictionary with help and version of API.
        """
        if version not in self.__versions:
            raise errors.NotFoundError('Version %s not found' % (version,))
        return [self.__help.get_help(version), version]

    def get_spec(self, version):
        """Get a well-formatted specification in RST format about available API
        resources.

        :param version: Version of API.
        :return: List of RST text and version of API.
        """
        if version not in self.__versions:
            raise errors.NotFoundError('Version %s not found' % (version,))
        return self.__help.get_spec(version)

    def __process_request(self, environ):
        """Helper for transforming HTTP(S) request into the call to
        an appropriate resource.

        :param environ: Request environment variable.
        :return: Response text, content type and result HTTP code.
        """
        user = environ.get('http_user')
        form = dict(paste.request.parse_formvars(environ))
        ctype = FORMAT.get(form.get('webapi_data_format', DEFAULT_FORMAT),
                           None)

        try:
            if not user:
                raise errors.LoginFailureError('Can\'t authenticate using '
                                               'provided credentials')
            # Must be a dequeue, since we add parameters at the beginning.
            url_params = deque()

            request_method = environ.get('REQUEST_METHOD').upper()
            meth_resource = self.__interface.get(request_method)
            if meth_resource is None:
                raise errors.InvalidData(
                        'HTTP request method is not supported: %s'
                        % (request_method,))

            method_data = None

            # Get full service resource identifier.
            # Depending on a server type it can be passed via different
            # variables.
            path = environ.get('PATH_INFO', None) \
                        or environ.get('SCRIPT_NAME', None)

            # Find the longest resource that may be called.
            res_id = path
            while res_id:
                internal_res_id = '%s:%s' % (res_id, len(url_params))
                if internal_res_id in meth_resource:
                    method_data = meth_resource[internal_res_id]
                    break
                else:
                    separated_url = res_id.rsplit('/', 1)

                    # Second element means we found a method's parameter.
                    if len(separated_url) == 2:
                        param = urllib.unquote_plus(separated_url[1])
                        url_params.appendleft(param)
                    res_id = separated_url[0]

            if method_data is None:
                raise errors.NotFoundError('Resource %s not found' % (path,))
            else:
                self.__check_permissions(method_data, user)
                data, code = self.__execute_method(method_data,
                                                   url_params,
                                                   form,
                                                   user)
        except (errors.WebAPIException, errors.AuthError), err:
            self.__log.info('%s: Exception: %s', user, self.__get_str_repr(err))
            data, code = self.__make_error(err)

        return data, ctype, code

    def __check_permissions(self, method_data, user):
        """Check whether a user has permissions to execute the method.

        :param method_data: Resource mapping.
        :param user: auth.User instance.
        """
        ldap_groups = method_data.get('LDAP_GROUPS')
        if not ldap_groups:
            return
        if not type(ldap_groups) in (list, tuple):
            ldap_groups = [ldap_groups]
        user.assert_privileges(*ldap_groups)

    def __execute_method(self, method_data, url_params, form, user):
        """Helper for parameters processing and method execution.

        :param method_data: Resource mapping.
        :param url_params: Parameters, passed via URL.
        :param form: Parameters passed in the request body.
        :param user: auth.User instance.
        :return: Response text and the result HTTP code.
        """
        kwargs = dict()
        for param, value in zip(method_data['URL_PARAMS'], url_params):
            name = param[0]
            validator = param[1]
            self.__log.debug('%s: Validation of "%s" parameter...',
                             user, name)
            kwargs[name] = validator(value)
            self.__log.debug('%s: Validation of "%s" parameter done.',
                             user, name)

        data_format = form.get('webapi_data_format', DEFAULT_FORMAT)
        if data_format != 'rencode':
            data_format = 'json'

        for param in method_data['QUERY_PARAMS']:
            name = param[0]
            validator = param[1]
            has_def_value = len(param) > 2

            if name in form:
                value = self.__decode_param(name, form[name], data_format)
                self.__log.debug('%s: Validation of "%s" parameter...',
                                 user, name)
                kwargs[name] = validator(value)
                self.__log.debug('%s: Validation of "%s" parameter done.',
                                 user, name)
            elif has_def_value:
                def_value = param[2]
                kwargs[name] = validator(def_value)
            elif validator.is_none_ok() is False:
                raise errors.InvalidData('Required parameter "%s"'
                                         ' must be specified.' % (name,))

        kwargs[USER_OBJECT] = user

        self.__log.info('%s: Request to: %s', user,
                        method_data['NAME'])
        self.__log.info('%s: Received arguments: %s', user, kwargs)

        # Remove unexpected parameters
        func = method_data['FUNCTION']
        func_args = inspect.getargspec(func)[0]

        valid_kwargs = dict()
        for arg in kwargs:
            if arg in func_args:
                valid_kwargs[arg] = kwargs[arg]

        data = func(**valid_kwargs)

        if method_data['HTTP_METHOD'] == 'GET':
            code = HTTP_200
        else:
            # For methods PUT, POST, DELETE '202 Accepted' will be returned.
            code = HTTP_202

        return data, code

    def __decode_param(self, name, data, data_format):
        """Helper for decoding query parameters from the appropriate format.

        :param name: Parameter name.
        :param data: Encoded data.
        :param data_format: Either 'json' or 'rencode'.
        :return: Decoded data.
        """
        try:
            if data_format == 'rencode':
                return rencode.loads(data)
            else:
                return cjson.decode(data)
        except Exception:
            raise errors.InvalidData('%s can\'t decode "%s" value' %
                                     (data_format, name))

    def __get_str_repr(self, obj):
        """Get string representation of an object.

        If object respresentation is a 'unicode' object
        (e.g. Exception(u'\xf6')), bytes not in range 0-128 will be
        relaced by their raw (hex) representation.

        :param obj: Object to be represented.
        :return: String (str object).
        """
        try:
            return str(obj)
        except ValueError:
            return unicode(obj).encode('raw_unicode_escape')

    def __make_error(self, err):
        """Helper for transforming known errors and defining HTTP return code.

        :param err: Exception instance.
        :param error_point: String with error point.
        :return: Error dictionary and result HTTP code.
        """
        # We need this due to a bug in common.auth.__init__
        if isinstance(err, errors.InsufficientPrivilegesError):
            err = errors.WebAPIInsufficientPrivilegesError(err.needed,
                                                           list(err.has))

        err_code = HTTP_500

        if isinstance(err, errors.DuplicationError):
            err_code = HTTP_409
        elif isinstance(err, errors.InvalidData):
            err_code = HTTP_406
        elif isinstance(err, errors.NotFoundError):
            err_code = HTTP_404
        elif isinstance(err, errors.WebAPIInsufficientPrivilegesError):
            err_code = HTTP_403
        elif isinstance(err, errors.LoginFailureError):
            err_code = HTTP_401

        err_dict = {'ERR_STR': self.__get_str_repr(err),
                    'ERR_CODE': err_code}

        return err_dict, err_code

    def __call__(self, environ, start_response):
        """Serialize the result of execution and generate the result page.

        :param environ: Request environment variable.
        :param start_response: Call-back function obtained from index.wsgi.
        :return: Result page to be sent to the client (inside a list).
        """

        result, c_type, ret_code = self.__process_request(environ)
        if c_type == JSON_FORMAT:
            self.__log.debug('Encoding data to JSON format...')
            page = cjson.encode(result)
            self.__log.info('Encoding to JSON format finished. '
                            'Data size: %s bytes', len(page))
        elif c_type == RENCODE_FORMAT:
            self.__log.debug('Encoding data to REncode format...')
            page = rencode.dumps(result)
            self.__log.info('Encoding to REncode format finished. '
                            'Data size: %s bytes', len(page))
        else:
            c_type = TEXT_FORMAT
            if type(result) in types.StringTypes:
                page = result
            else:
                page = pprint.pformat(result)

        start_response(ret_code, [('Content-type', c_type)])
        return [page]


class HelpGen(object):

    """Helper class for help and specification generation."""

    def __init__(self):
        self.__help_interface = dict()
        self.__spec = dict()

    def get_help(self, version):
        """Get generated help.

        :param version: Version of API.
        :return: Dictionary with help data.
        """
        help_data = self.__help_interface.get(version)
        help_result = dict()

        for module_name, module_info in help_data.iteritems():
            module_help_result = help_result.setdefault(module_name, dict())

            for res_name, res_info in module_info.iteritems():
                res_help_result = module_help_result.setdefault(res_name,
                                                                dict())

                # Just copy needed values from base help data.
                res_help_result['DESCRIPTION'] = res_info['DESCRIPTION']
                res_help_result['DOCSTRING'] = res_info['DOCSTRING']
                res_help_result['HTTP_METHOD'] = res_info['HTTP_METHOD']
                res_help_result['HTTP_URL'] = res_info['HTTP_URL']
                res_help_result['PARAMETERS'] = res_info['PARAMETERS']
                res_help_result['RETURN'] = res_info['RETURN']
                res_help_result['SHORT_DOCSTRING'] = \
                    res_info['SHORT_DOCSTRING']

        return help_result

    def get_spec(self, version):
        """Get well-formatted specification in RST format about API.

        :param version: Version of resources.
        :return: API help as RST text data.
        """
        help_data = self.__help_interface.get(version)
        final_spec = list()

        for module_name, module_help in help_data.iteritems():
            final_spec.append(module_name)
            final_spec.append('-' * len(module_name))
            final_spec.append('')

            for res_name, res_help in module_help.iteritems():
                final_spec.append(res_help['DESCRIPTION'])
                final_spec.append('`' * len(res_help['DESCRIPTION']))
                final_spec.append('')
                final_spec.append(':Doc: ' + res_help['SHORT_DOCSTRING'])
                final_spec.append(':URL: ' + res_help['HTTP_URL'])
                final_spec.append(':HTTP method: ' + res_help['HTTP_METHOD'])

                final_spec.append(':Function signature: api_instance.%s%s' % \
                                  (res_name, res_help['SIGNATURE']))

                for param_info in res_help['PARAMETERS']:
                    if param_info[2].lower() == 'optional':
                        extra_help = 'This parameter is optional.'
                    else:
                        extra_help = 'This parameter must be specified.'
                    final_spec.append(':Parameter %s: %s%s' % (param_info[0],
                                                               param_info[1],
                                                               extra_help))

                if res_help['RETURN'].find('\n') >= 0:
                    final_spec.append(':Return:')
                    final_spec.append('')
                    final_spec.append('::')
                    final_spec.append('')
                    final_spec.append('    ' + res_help['RETURN'].strip())
                else:
                    final_spec.append(':Return: %s' % \
                                      res_help['RETURN'].strip())
                final_spec.append('')
                final_spec.append('')

        return '\n'.join(final_spec)

    def extend_help(self, res_data, version):
        """Extend existing help with information about the new resource.

        :param res_data: Dictionary with resource mapping.
        :param version: Version of API.
        """
        version_help = self.__help_interface.setdefault(version, dict())

        func_module = inspect.getmodule(res_data['FUNCTION'])
        if hasattr(func_module, 'MODULE_NAME'):
            module_name = getattr(func_module, 'MODULE_NAME')
        else:
            module_name = ''

        res_help = version_help.setdefault(module_name, dict()).\
                                setdefault(res_data['NAME'], dict())

        func_doc, params_doc = self.__parse_function_help(
                                                res_data['FUNCTION'])

        res_help['RETURN'] = params_doc.get('return', 'N/A')
        res_help['SHORT_DOCSTRING'] = func_doc['short']
        res_help['DOCSTRING'] = '%s\n\n%s' % (func_doc['short'],
                                              func_doc['long'])

        res_help['PARAMETERS'], res_help['SIGNATURE'] = \
                        self.__process_params_help(res_data, params_doc)

        res_help['HTTP_URL'] = res_data['HTTP_URL']
        res_help['HTTP_METHOD'] = res_data['HTTP_METHOD']
        res_help['DESCRIPTION'] = res_data['DESCRIPTION']

    def __parse_function_help(self, function):
        """Helper for extracting method help.

        :param function: Function object.
        :return: Dictionary with help information.
        """
        help_text = inspect.getdoc(function)

        if help_text:
            h_data = help_text.split('\n\n', 2)
        else:
            h_data = list()

        func_help = {'short': '',
                     'long': ''}
        params_help = {}

        if len(h_data) > 0:
            func_help['short'] = self.__reformat_str(h_data[0])

        if len(h_data) == 3:
            func_help['long'] = h_data[1]

        if len(h_data) > 1:
            params_help = self.__extract_params_help(h_data[-1])

        return func_help, params_help

    def __process_params_help(self, res_data, params_doc):
        """Helper for parameters analysis.

        :param res_data: Dictionary with resource mapping.
        :param param_doc: Parameters docstring from internal function.
        :return: Lists with help information about parameters
                 and the method signature string.
        """
        params_help = list()
        sign_info = list()

        for name, validator in res_data['URL_PARAMS']:
            param_help = list()
            param_help.append(name)
            param_help.append(params_doc.get(name, '') + validator.get_help())
            param_help.append('required')
            param_help.append('URL parameter')
            params_help.append(param_help)
            sign_info.append(name)

        for param_info in res_data['QUERY_PARAMS']:
            name = param_info[0]
            validator = param_info[1]

            if len(param_info) > 2:
                has_def_value = True
                sign_info.append('%s=%s' % (name, param_info[2]))
            else:
                sign_info.append(name)
                has_def_value = False

            param_help = list()
            param_help.append(name)
            param_help.append(params_doc.get(name, '') + validator.get_help())
            if not validator.is_none_ok() and not has_def_value:
                param_help.append('required')
            else:
                param_help.append('optional')
            param_help.append('query parameter')
            params_help.append(param_help)

        return params_help, '(%s)' % (', '.join(sign_info))

    def __extract_params_help(self, params_help):
        """Helper for extracting parameters help.

        :param params_help: Docstring text with a description of
                            parameters and return data.
        :return: Dictionary with help information about parameters
                 and return data.
        """
        pr = params_help.split(':return:', 1)
        orig_params_help = dict()

        if len(pr) == 2:
            p_text, r_text = pr
        elif len(pr) == 1:
            p_text = pr[0]
            r_text = 'N/A'
        else:
            return orig_params_help

        # Store RETURN help
        orig_params_help['return'] = r_text

        # Process parameters help
        for i in p_text.split(':param '):
            s_data = i.split(':', 1)
            if len(s_data) == 2:
                pn, pd = s_data
                pd = self.__reformat_str(pd)
                # Do this to save formatting for last parameter's description
                if not pd.endswith(' '):
                    pd += ' '
                orig_params_help[pn] = pd

        return orig_params_help

    def __reformat_str(self, str_data):
        """Helper for string reformating.

        :param str_data: Docstring text with a method's description.
        :return: Reformatted text.
        """
        return ' '.join([i.strip() for i in str_data.split('\n')])
