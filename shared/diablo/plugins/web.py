"""Web interface for Diablo.

Plugin for Diablo that starts a web server and runs a WSGI application that
allows methods to be called by name in the URL.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/web.py#16 $
"""

import errno
import logging
import pprint
import select
import traceback
import urlparse
import wsgiref.headers
import wsgiref.simple_server

import shared.util.decoratorutils
import shared.web.html_formatting

from shared.diablo.decorator_registrar import DecoratorRegistrar, bind
from shared.diablo.plugins import DiabloPlugin, generate_port

BASE_PORT = 80


class WebPlugin(DiabloPlugin):
    """Plugin for Diablo that starts a web server and runs a WSGI app.

    To enable this plugin put this line in your Diablo application __init__:

    self.register_plugin(shared.diablo.plugins.web.WebPlugin)

    To add methods to the interface use the "web_method" decorator.
    """
    NAME = 'web'

    def __init__(self, *args, **kwargs):
        """Initialize methods list."""

        self.__log = logging.getLogger('diablo.plugins.web')

        super(WebPlugin, self).__init__(*args, **kwargs)

        self.port = None
        self.server = None
        self._methods = {}

    def startup(self):
        """Start the web server."""

        conf_str = '%s.web_server_port' % (self.conf_section,)
        conf_port = self.conf.get(conf_str)
        if conf_port:
            self.port = conf_port + self.daemon_obj.node_id
        else:
            self.port = generate_port(BASE_PORT, self.daemon_obj.node_id,
                                      self.daemon_obj.app_name)

        # Bind web methods to daemon object.
        self._bind_web_methods(self.daemon_obj)

        # Bind web methods to plugin instances.
        for name in self.daemon_obj._plugins:
            if name == self.NAME:
                continue
            plugin_obj = self.daemon_obj._plugins[name]
            self._bind_web_methods(plugin_obj)

        self.server = wsgiref.simple_server.make_server(
                            '', self.port, self.get_app(),
                            server_class=WSGIServerReuseAddr,
                            handler_class=LoggingWSGIRequestHandler)

        self.__log.info('Web server started on port [%d].', self.port)
        self.__log.debug('Web server has these methods:\n%s', self._methods)

        self.start_in_thread('web_server', self._web_serve_forever)

    def _web_serve_forever(self):
        """Web server loop, breaks when the daemon shuts down."""
        while True:
            try:
                self.server.serve_forever()
            except select.error, exc:
                if exc[0] == errno.EINTR:
                    # errno == EINTR appears when the process is killed.
                    # When this happened, just retry. The error condition
                    # will clear up.
                    self.__log.warn('Select call interrupted')

                    # Continue to serve in order to prevent possible deadlock
                    # in BaseServer shutdown call:
                    # Python Standard Library, SocketServer.py, version 0.4
                    continue
                else:
                    raise

            if not self.daemon_obj.should_continue():
                # Break if we've done.
                break

    def _bind_web_methods(self, obj):
        """Bind available web methods."""
        for func, (name, hide, direct) in getattr(obj, 'WEB_METHODS', []):
            if name in self._methods:
                # Duplicate method, take the first one.
                continue
            self._methods[name] = (bind(func, obj), hide, direct)

    def get_app(self):
        """Returns the WSGI application to use.

        Override in subclass to use a different application or to add
        middleware.
        """
        return DiabloWSGIApp(self._methods, self.daemon_obj)

    def get_status(self):
        """Return a dictionary of status information."""

        return {'port': self.port,
                'methods': list(self._methods)}

    def shutdown(self):
        """Stop web server."""

        self.__log.info('Shutting down web server.')
        self.server.shutdown()
        self.join_threads()
        self.server.server_close()


def format_uptime(uptime):
    """Make human readable string about uptime

    Returns a string like "1 Days, 8 Hours, 23 Minutes, 42.2 Seconds"

    Leading syllables that are zero are omitted
    e.g. 74.0 --> "1 Minutes, 14.0 Seconds"
    """
    seconds = float(uptime)

    if not seconds:
        return 'Not Running'

    syllables = []

    syllables.append(('Days', int(seconds / 86400)))
    seconds = seconds % 86400

    syllables.append(('Hours', int(seconds / 3600)))
    seconds = seconds % 3600

    syllables.append(('Minutes', int(seconds / 60)))
    seconds = seconds % 60

    ret = []
    leading_zero = True
    for name, val in syllables:
        if leading_zero and val == 0:
            continue
        else:
            # Found first non-zero syllable, print all from now on.
            leading_zero = False

        # Remove trailing s if 1.
        if val == 1:
            name = name[:-1]

        ret.append('%d %s, ' % (val, name))

    ret.append('%.1f Seconds' % (seconds,))
    return ''.join(ret)


@shared.util.decoratorutils.allow_no_args_form
def web_method(name=None, hide=False, direct=False):
    """Set up a web method.

    Used as a decorator to indicate this method provides something that should
    be accessible via the daemon's Web interface.

    ``name`` is the name of the method and is also the URL used to access it.

    If ``hide`` is set to True, the daemon should not include it in the list of
    available methods.

    If ``direct`` is set to True, it is expected to product a full HTML
    output. It will not be wrapped in the standard header and footer.

    The decorated method must have one parameter called "params".  This is
    the result of ``cgi.parse_qs``.  If it was a GET request, it comes from the
    query string.  If it was a POST request, it comes from the message body.

    If multiple methods in the same class specify the same name, the result is
    undefined.  If a subclass uses the same name as a parent class, the
    subclass's method will be used.
    """

    def decorator(func):
        """Register the function."""

        method_name = name
        if method_name is None:
            method_name = func.__name__

        DecoratorRegistrar.register('WEB_METHODS', func,
                                    (method_name, hide, direct))
        return func
    return decorator


class WSGIServerReuseAddr(wsgiref.simple_server.WSGIServer):

    # Allow quick restarts without waiting for TIME_WAIT of socket.
    allow_reuse_address = True


# The method to redirect to on a request for "/"
DEFAULT_METHOD_NAME = 'status'

class DiabloWSGIApp(object):
    """Very simple wsgi app that call methods based on path."""

    def __init__(self, methods, daemon_obj):
        """Store instance variables."""
        self.__log = logging.getLogger('diablo.plugins.web')

        self._methods = methods
        self.daemon_obj = daemon_obj

    def __call__(self, environ, start_response):
        """Service a request.

        Handle exceptions and status codes.
        """

        if self.__log.isEnabledFor(logging.DEBUG):
            self.__log.debug('Recieved request:\n%s', pprint.pformat(environ))

        resp_headers = wsgiref.headers.Headers([])
        try:
            status, ret_val = self.handle_request(environ, resp_headers)
        except Exception:
            status = STATUS[500]
            content = '<h2>Error occured processing request</h2>'
            content += '<br /><br /><pre>%s</pre>' % (traceback.format_exc(),)
            ret_val = self.wrap_html(content, 'ERROR')

            self.__log.exception('Error handling request:\n%s',
                            pprint.pformat(environ))

        start_response(status, resp_headers.items())

        return [ret_val]

    def handle_request(self, environ, resp_headers):
        """Call the method given in the path."""

        method_name = environ['PATH_INFO'].strip('/')

        if not method_name:
            resp_headers.add_header('Location', '/' + DEFAULT_METHOD_NAME)
            return STATUS[301], ''

        if method_name not in self._methods:
            content = '<h4>Unknown method "%s"</h4>' % (method_name,)
            ret_val = self.wrap_html(content, 'ERROR')
            return STATUS[404], ret_val

        if environ['REQUEST_METHOD'] == 'POST':
            length = int(environ['CONTENT_LENGTH'])
            params = urlparse.parse_qs(environ['wsgi.input'].read(length),
                                       keep_blank_values=True)
        else:
            params = urlparse.parse_qs(environ['QUERY_STRING'],
                                       keep_blank_values=True)

        method, hide, direct = self._methods[method_name]
        result = method(params)

        if direct:
            resp_headers.add_header('Content-Type', 'text/plain')
        else:
            result = self.wrap_html(result, method_name)
            resp_headers.add_header('Content-Type', 'text/html')

        return STATUS[200], result

    def wrap_html(self, content, method):
        """Wrap content with header and footer."""

        return '\n'.join([self.get_header(method), content, self.get_footer()])

    def get_header(self, method):
        """Return the HTML for the header of a page."""

        head = []
        head.append('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 '
                    'Transitional//EN" "http://www.w3.org/TR/html4/'
                    'loose.dtd">\n')
        head.append('<html><head>\n')
        head.append('<meta http-equiv="Content-Type" content="text/html">\n')
        head.append('<style type="text/css">')
        head.append('<!--\n%s\n--></style>' % (self.get_style(),))
        head.append('<title>%s</title>\n' % (self.get_title(method),))
        head.append('</head><body>\n')
        head.append('<div id="header">\n')

        fields = [('App Name', self.daemon_obj.app_name),
                  ('Hostname', self.daemon_obj.hostname),
                  ('Node', self.daemon_obj.node_id),
                  ('PID', self.daemon_obj.pid),
                  ('Version', self.daemon_obj.VERSION),
                  ('Daemon Uptime',
                        format_uptime(self.daemon_obj.get_daemon_uptime())),
                  ('Application Uptime',
                        format_uptime(self.daemon_obj.get_app_uptime()))]

        # Get status info for other plugins.
        status = self.daemon_obj.get_status()
        try:
            rpc_port = status['plugins']['rpc']['port']
            fields.append(('RPC Port', rpc_port))
        except KeyError:
            pass

        try:
            backdoor_port = status['plugins']['backdoor']['port']
            fields.append(('Backdoor Port', backdoor_port))
        except KeyError:
            pass

        field_names = [x[0] for x in fields]
        values = [x[1] for x in fields]
        head.append(shared.web.html_formatting.list_of_lists_to_table(
                                    [field_names, values], table_class='status'))

        head.append('<div id="nav">\n')
        head.append('<b>Methods:</b>&nbsp;&nbsp&nbsp')

        links = []
        for name, (func, hide, direct) in self._methods.iteritems():
            if not hide:
                links.append('<a href="/%s">%s</a>' % (name, name))

        head.append('&nbsp;&nbsp|&nbsp;&nbsp'.join(links))
        head.append('</div></div>\n')
        head.append('<div id=content>\n')

        return ''.join(head)

    def get_style(self):
        """Return the CSS style directives to use on the page.

        Subclasses may add their own style by overriding this method
        and adding their own directives.
        """
        return """
                 table {
                 border-collapse:collapse;
                 }
                 body {
                 margin:0;
                 padding:0;
                 }
                 table, td, th {
                 border:1px solid gray;
                 padding:3px;
                 }
                 th {
                 background-color:#CCCCCC;
                 }
                 td {
                 background-color:#FFFFFF;
                 }
                 .alt td {
                 background-color:#EEEEEE;
                 }
                 .status {
                 margin-bottom:10px;
                 width:100%;
                 }
                 .status th, .status td {
                 padding:3px;
                 text-align:center;
                 }
                 .status th {
                 background-color:#FFFFB3;
                 }
                 #nav {
                 background-color:#FFD9B3;
                 border:1px solid gray;
                 font-size:1.1em;
                 padding:3px;
                 }
                 #header {
                 background-color:#F0F0F0;
                 margin:0 0 20px;
                 padding:10px;
                 border-bottom:1px dotted gray;
                 }
                 #content {
                 padding:10px;
                 }
               """

    def get_title(self, method):
        """Return the title for the page."""

        return '%s - [%s:%s] - %s [node:%d]' % \
                      (method, self.daemon_obj.app_name,
                       self.daemon_obj.VERSION, self.daemon_obj.hostname,
                       self.daemon_obj.node_id)

    def get_footer(self):
        """Return the HTML for the end of a page."""

        return '</div></body></html>\n'


STATUS = {200: '200 OK',
          301: '301 Moved Permanently',
          404: '404 Not Found',
          500: '500 Internal Server Error'
         }


class LoggingWSGIRequestHandler(wsgiref.simple_server.WSGIRequestHandler):
    """Override log methods in BaseHTTPRequestHandler.

    Output to log instead of stderr.  It seems crazy this actually uses stderr.
    """

    def log_request(self, code='-', size='-'):
        """Do nothing.  We will log requests in the WSGI application."""

        pass

    def log_error(self, format, *args):
        """Just log at error level."""

        log = logging.getLogger('diablo.plugins.web')
        log.error(format, *args)
