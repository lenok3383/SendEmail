"""Helper methods to format python objects into HTML.

:Status: $Id: //prod/main/_is/shared/python/web/html_formatting.py#5 $
:Authors: scottrwi
"""

import cgi


def list_of_lists_to_table(list_of_list, table_class=None, escape=True,
                           highlight_first_row=True):
    """Return an HTML table representation of a list of lists.

    Though it's called list of lists it's really an iterable of iterables.
    You can pass in list of tuples or a tuple of tuples.

    Every other row will have the class "alt" assigned to the <tr> tag.

    :param list_of_list:  The object to be formatted.
    :param table_class: The string to assign to the class attribute of the
                        table tag.
    :param escape: If True, calls cgi.escape() on each element in the table.
    :param highlight_first_row: Tells whether to use the <th> tag for the
                                first row.

    :return: String of HTML
    """
    html = ''
    html += '<table'
    if table_class:
        html += ' class="%s"' % (table_class,)
    html += '>\n'

    for i, row in enumerate(list_of_list):
        if i % 2:
            html += '<tr>\n'
        else:
            html += '<tr class="alt">\n'

        for d in row:
            s = str(d)
            if escape:
                s = cgi.escape(s)
            if not s:
                s = '&nbsp;'
            if i == 0 and highlight_first_row:
                html += '<th>%s</th>\n' % (s,)
            else:
                html += '<td>%s</td>\n' % (s,)

        html += '</tr>\n'

    html += '</table>\n'

    return html


def dict_to_table(dic, table_class=None, escape=True):
    """Format the contents of a dictionary as a two column HTML table.

    :param dic:  A dictionary to be formatted
    :param table_class: The string to assign to the class attribute of the
                        table tag.
    :param escape: If True, calls cgi.escape() on each element in the table.

    :return: String of HTML
    """
    lol = sorted(dic.items())
    lol.insert(0, ('Key', 'Value'))

    return list_of_lists_to_table(lol, table_class=table_class, escape=escape,
                                  highlight_first_row=True)


