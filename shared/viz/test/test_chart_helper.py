from cStringIO import StringIO

import unittest2 as unittest

import shared.viz.chart_helper

class ChartHelperTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def tearDown(self):
        pass

    def test_scale_data_positive_only(self):
        unscaled_data = range(0, 100, 10)
        scaled_data = shared.viz.chart_helper.scale_data(unscaled_data, 9)
        self.assertSequenceEqual(scaled_data, range(0, 10, 1))

    def test_scale_data_negative(self):
        unscaled_data = range(-90, 100, 20)
        scaled_data = shared.viz.chart_helper.scale_data(unscaled_data, 9)
        self.assertSequenceEqual(scaled_data, range(0, 10, 1))

    def test_histogram_no_step(self):
        # every other element exists twice
        histogram_data = range(10) + range(0, 10, 2)
        hist, mins, maxs = shared.viz.chart_helper.histogram(histogram_data, 1)
        self.assertSequenceEqual(hist, [2, 1, 2, 1, 2, 1, 2, 1, 2, 1])
        self.assertSequenceEqual(mins, range(0, 10, 1))
        self.assertSequenceEqual(maxs, range(1, 11, 1))

    def test_histogram_step(self):
        histogram_data = range(10) + range(0, 10, 2)
        hist, mins, maxs = shared.viz.chart_helper.histogram(histogram_data, 2)
        self.assertSequenceEqual(hist, [3, 3, 3, 3, 3])
        self.assertSequenceEqual(mins, range(0, 10, 2))
        self.assertSequenceEqual(maxs, range(2, 12, 2))

    def test_histgram_min(self):
        histogram_data = range(10) + range(0, 10, 2)
        hist, mins, maxs = shared.viz.chart_helper.histogram(histogram_data, 1, 5)
        self.assertSequenceEqual(hist, [1, 2, 1, 2, 1])
        self.assertSequenceEqual(mins, range(5, 10, 1))
        self.assertSequenceEqual(maxs, range(6, 11, 1))

    def test_print_histogram(self):
        histogram_data = range(10) + range(0, 10, 2)
        hist, mins, maxs = shared.viz.chart_helper.histogram(histogram_data, 1)
        labels = ['%s>=x<%s' % (mn, mx) for mn, mx in zip(mins, maxs)]
        out = StringIO()
        shared.viz.chart_helper.print_histogram(hist, labels, output=out)
        goal = """\
0.0>=x<1.0:\t**
1.0>=x<2.0:\t*
2.0>=x<3.0:\t**
3.0>=x<4.0:\t*
4.0>=x<5.0:\t**
5.0>=x<6.0:\t*
6.0>=x<7.0:\t**
7.0>=x<8.0:\t*
8.0>=x<9.0:\t**
9.0>=x<10.0:\t*
"""
        self.assertMultiLineEqual(out.getvalue(), goal)

    def test_gviz_simple_chart_line(self):
        data = [('label 1', 1), ('label 2', 2), ('label 3', 3)]
        html = shared.viz.chart_helper.gviz_simple_chart(
            data, xlabels_column=0, series_labels=['name', 'value'],
            chart_type=shared.viz.chart_helper.LINE_CHART,
            title='test', is3D=True)
        goal = """\
<div id="chart_1"></div>
<script type="text/javascript">
google.load("visualization", "1", {packages:["linechart"]});
google.setOnLoadCallback(drawChart);
function drawChart() {
var data = new google.visualization.DataTable();
data.addColumn("string", "name");
data.addColumn("number", "value");
data.addRows(3);
data.setValue(0, 0, "label 1");
data.setValue(0, 1, 1);
data.setValue(1, 0, "label 2");
data.setValue(1, 1, 2);
data.setValue(2, 0, "label 3");
data.setValue(2, 1, 3);
var chart = new google.visualization.LineChart(document.getElementById("chart_1"));
chart.draw(data, {width: 640, height: 480, legend: "bottom", title: "test", is3D: true});
}</script>
"""
        self.assertMultiLineEqual(html, goal)

if __name__ == '__main__':
    unittest.main()
