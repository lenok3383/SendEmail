"""chart_helper.py

Creates charts using Google's chart API.

http://code.google.com/apis/chart/

:Status: $Id: //prod/main/_is/shared/python/viz/chart_helper.py#4 $
:Authors: bjung
"""

import sys
import threading

V_BAR_CHART = 'bvg'
H_BAR_CHART = 'bhg'
LINE_CHART  = 'lc'

_CHART_ID_LOCK = threading.RLock()
_chart_id = 0


class ChartDataError(Exception):
    """Chart creation error."""

    pass


def import_js_libs():
    """Requred javascript library for gviz_simple_chart."""

    return """<script type="text/javascript" src="http://www.google.com/jsapi"></script>"""


def scale_data(data, scale_to):
    """Scale data points to 0...<scale_to>.

    :param data: a list of values
    :param scale_to: maximum value of scaled data
    """
    scaled_data = []
    scale_to = float(scale_to)
    data_min = float(min(data))
    data_max = float(max(data))

    for point in data:
        try:
            if data_min < 0.0:
                scaled_point = scale_to * (point - data_min) / \
                    (data_max - data_min)
            else:
                scaled_point = scale_to * point / data_max
        except ZeroDivisionError:
            scaled_point = 0.0
        scaled_data.append(scaled_point)

    return scaled_data


def histogram(data, step, minimum=0.0):
    """Return a tuple of three lists which represent a histogram.  The
    first item is the a list of the minimum boudries which define the
    histogram, the second is a list of the maximum boundries which
    define the histogram.  Lastly the third list is the counts of data
    within those boudaries.  For example, the following could describe
    data which falls between 0 and 3:

    ([0, 1, 2], [1, 2, 3], [2, 6, 8])

    :param data: a list of values
    :param step: distance between histgram points
    :param minimum: a floor for the histogram
    """

    hist = list()
    mins = list()
    maxs = list()
    if data:
        data_max = max(data)
        i = minimum
        while i <= data_max:
            items = filter(lambda x: x >= i and x < i + step, data)
            hist.append(len(items))
            mins.append(i)
            maxs.append(i + step)
            i += step
    return hist, mins, maxs


def unique_values(data):
    """Return a sorted list of unique values.

    :param data: a list of values
    """
    return sorted(set(data))


def print_histogram(data, labels, resolution=1, output=sys.stdout):
    """Print a graphical representation of a histogram to stdout.

    :param data: a list of values
    :param labels: labels for histogram point
    :param resolution: resolution of histogram values (integer)
    """
    assert resolution > 0 and isinstance(resolution, int), \
        'resolution must be an integer >= 1'
    for i in xrange(len(data)):
        stars = '*' * (int(data[i]) / resolution)
        output.write('%s:\t%s\n' % (str(labels[i]), stars))


def gviz_simple_chart(data, xlabels_column=-1, series_labels=None, xsize=640,
                      ysize=480, chart_type=V_BAR_CHART, title='',
                      **chart_options):
    """A function for generating google visualization simple charts.

    These charts are rendered in the browser and do not send data back to
    google.

    THE HTML OUTPUT DEPENDS ON THE JAVASCRIPT LIBRARIES BEING PRESENT (see
    `import_js_libs`)

    Example usage:

        data = [('12/10/09', 10), ('12/10/09', 7), ('12/10/09', 14)]

        chart_helper.gviz_simple_chart(
            data, xlabels_column=0, series_labels=['date', 'value'],
            chart_type=chart_helper.H_BAR_CHART, title='date vs. value',
            is3D=True)

    :param data: a sequence of sequences, or list of rows:
        [(x_value_1, y_series1_value_1, y_series2_value_1),
         (x_value_2, y_series1_value_2, y_series2_value_2)]
    :param xlabels_column: the column, in the data which corresponds to the
        x values/labels.  For the above example xlabels_column=0
    :param series_labels: a human readable name for the data series/columns.
        For example, if you were plotting time vs. rate,
        series_labels=['time', 'rate]
    :param xsize: the horizontal size in pixels of the plotted chart
    :param ysize: the vertical size in pixels of the plotted chart
    :param chart_type: V_BAR_CHART|H_BAR_CHART|LINE_CHART
    :param title: the chart title
    :param **kwargs: additional keyword arguments which are passed to the
        chart drawing command, 'is3D=True', for instance. See:
        http://code.google.com/apis/visualization/documentation/gallery/barchart.html
    """
    global _chart_id

    chart_map = {V_BAR_CHART: {'package': 'columnchart',
                               'visualization': 'ColumnChart'},
                 H_BAR_CHART: {'package': 'barchart',
                               'visualization': 'BarChart'},
                 LINE_CHART: {'package': 'linechart',
                              'visualization': 'LineChart'}}

    if not isinstance(data[0], (list, tuple)):
        # If we just feed in a sequence of values to plot (not a sequence of
        # sequences).
        data = [[d] for d in data]

    if series_labels is not None and len(series_labels) != len(data[0]):
        raise ChartDataError(
            'number of column labels does not match number of data columns %s '
            'vs. %s' % (len(series_labels), len(data[0]),))

    if not series_labels:
        series_labels = [''] * len(data[0])
    data_values = []
    data_columns = []

    if xlabels_column > len(data[0]) - 1:
        raise ChartDataError(
            'x label column greater than number of data columns %s vs. %s' % (
            xlabels_column, len(data[0]),))

    # Prepare the data points.
    for xindex, data_points in enumerate(data):
        # Convert data from tuple to list (to make mutable).
        data_points = list(data_points)
        if xlabels_column >= 0:
            xlabel = data_points[xlabels_column]
            data_points.pop(xlabels_column)
            data_values.append('data.setValue(%s, %s, "%s");' % (
                xindex, 0, xlabel,))
        for yindex, value in enumerate(data_points):
            if xlabels_column >= 0:
                # Increment yindex since first value is a data label.
                yindex += 1
            data_values.append('data.setValue(%s, %s, %s);' % (
                xindex, yindex, value,))

    # Handle the x column.
    if xlabels_column >= 0:
        data_columns.append('data.addColumn("string", "%s");' % (
            series_labels[xlabels_column]))
        if isinstance(series_labels, tuple):
            series_labels = list(series_labels)
        series_labels.pop(xlabels_column)

    # Add each y series column.
    for label in series_labels:
        data_columns.append('data.addColumn("number", "%s");' % (label,))

    options = []
    for opt, value in chart_options.iteritems():
        if isinstance(value, str):
            options.append('%s: "%s"' % (opt, value,))
        else:
            if isinstance(value, bool):
                options.append('%s: %s' % (opt, str(value).lower(),))
            else:
                options.append('%s: %s' % (opt, value,))

    with _CHART_ID_LOCK:
        _chart_id += 1

    html = \
"""<div id="chart_%s"></div>
<script type="text/javascript">
google.load("visualization", "1", {packages:["%s"]});
google.setOnLoadCallback(drawChart);
function drawChart() {
var data = new google.visualization.DataTable();
%s
data.addRows(%s);
%s
var chart = new google.visualization.%s(document.getElementById("chart_%s"));
chart.draw(data, {width: %s, height: %s, legend: "bottom", title: "%s", %s});
}</script>
""" % (_chart_id, chart_map[chart_type]['package'], '\n'.join(data_columns),
        len(data), '\n'.join(data_values),
        chart_map[chart_type]['visualization'], _chart_id, xsize, ysize, title,
        ','.join(options))
    return html

