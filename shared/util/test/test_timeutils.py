
import datetime
import time
import unittest2

from shared.util import timeutils as t


# Mockup GMT time values.
NOW = 1283378386 # 1.09.2010
NEXT_MONTH = 1285891200 # 2010-10-01 00:00:00
THIS_MONTH = 1283299200 # 2010-09-01 00:00:00
LAST_MONTH = 1280620800 # 2010-08-01 00:00:00
TWO_MONTHS_AGO = 1277942400 # 2010-07-01 00:00:00
WEEK_AGO = 1282694400 # 2010-08-25 00:00:00
TODAY = 1283299200 # 2010-09-01 00:00:00
YESTERDAY = 1283212800 # 2010-08-31 00:00:00

TIME_ZONE_SHIFT = 25200 # time.alzone


class TimeTest(unittest2.TestCase):
    """Test case class to test timeutils module."""

    time_time_mockup = lambda x: NOW

    def setUp(self):
        self.orig_time_func = t.time.time
        t.time.time = self.time_time_mockup

    def tearDown(self):
        t.time.time = self.orig_time_func

    def test_nextmonth_gmt(self):
        self.assertEqual(t.nextmonth_gmt(), NEXT_MONTH)
        self.assertEqual(t.nextmonth_gmt(NOW + t.ONEHOUR), NEXT_MONTH)
        self.assertNotEqual(t.nextmonth_gmt(NOW - t.ONEMONTH), NEXT_MONTH)

    def test_nextmonth(self):
        local_nextmonth = NEXT_MONTH + TIME_ZONE_SHIFT
        self.assertEqual(t.nextmonth(), local_nextmonth)
        self.assertEqual(t.nextmonth(NOW + t.ONEHOUR), local_nextmonth)
        self.assertNotEqual(t.nextmonth(NOW - t.ONEMONTH), local_nextmonth)

    def test_thismonth_gmt(self):
        self.assertEqual(t.thismonth_gmt(), THIS_MONTH)
        self.assertEqual(t.thismonth_gmt(NOW + t.ONEHOUR), THIS_MONTH)
        self.assertNotEqual(t.thismonth_gmt(NOW - t.ONEMONTH), THIS_MONTH)

    def test_thismonth(self):
        local_thismonth = THIS_MONTH + TIME_ZONE_SHIFT
        self.assertEqual(t.thismonth(), local_thismonth)
        self.assertEqual(t.thismonth(NOW + t.ONEHOUR), local_thismonth)
        self.assertNotEqual(t.thismonth(NOW - t.ONEMONTH), local_thismonth)

    def test_twomonthsago_gmt(self):
        self.assertEqual(t.twomonthsago_gmt(), TWO_MONTHS_AGO)
        self.assertEqual(t.twomonthsago_gmt(NOW + t.ONEHOUR),
                                              TWO_MONTHS_AGO)
        self.assertNotEqual(t.twomonthsago_gmt(NOW - t.ONEMONTH),
                            TWO_MONTHS_AGO)

    def test_twomonthsago(self):
        local_twomonthsago = TWO_MONTHS_AGO + TIME_ZONE_SHIFT
        self.assertEqual(t.twomonthsago(), local_twomonthsago)
        self.assertEqual(t.twomonthsago(NOW + t.ONEHOUR), local_twomonthsago)
        self.assertNotEqual(t.twomonthsago(NOW - t.ONEMONTH),
                            local_twomonthsago)

    def test_lastmonth_gmt(self):
        self.assertEqual(t.lastmonth_gmt(), LAST_MONTH)
        self.assertEqual(t.lastmonth_gmt(NOW + t.ONEHOUR), LAST_MONTH)
        self.assertNotEqual(t.lastmonth_gmt(NOW - t.ONEMONTH), LAST_MONTH)

    def test_lastmonth(self):
        local_last_month = LAST_MONTH + TIME_ZONE_SHIFT
        self.assertEqual(t.lastmonth(), local_last_month)
        self.assertEqual(t.lastmonth(NOW + t.ONEHOUR), local_last_month)
        self.assertNotEqual(t.lastmonth(NOW - t.ONEMONTH), local_last_month)

    def test_weekago_gmt(self):
        self.assertEqual(t.weekago_gmt(), WEEK_AGO)
        self.assertEqual(t.weekago_gmt(NOW + t.ONEHOUR), WEEK_AGO)
        self.assertNotEqual(t.weekago_gmt(NOW - t.ONEWEEK), WEEK_AGO)

    def test_weekago(self):
        local_weekago = WEEK_AGO + TIME_ZONE_SHIFT
        self.assertEqual(t.weekago(), local_weekago)
        self.assertEqual(t.weekago(NOW + t.ONEHOUR), local_weekago)
        self.assertNotEqual(t.weekago(NOW - t.ONEWEEK), local_weekago)

    def test_today_gmt(self):
        self.assertEqual(t.today_gmt(), TODAY)
        self.assertEqual(t.today_gmt(NOW + t.ONEHOUR), TODAY)
        self.assertNotEqual(t.today_gmt(NOW - t.ONEDAY), TODAY)

    def test_today(self):
        local_today = TODAY + TIME_ZONE_SHIFT
        self.assertEqual(t.today(), local_today)
        self.assertEqual(t.today(NOW + t.ONEHOUR), local_today)
        self.assertNotEqual(t.today(NOW - t.ONEDAY), local_today)

    def test_yesterday_gmt(self):
        self.assertEqual(t.yesterday_gmt(), YESTERDAY)
        self.assertEqual(t.yesterday_gmt(NOW + t.ONEHOUR), YESTERDAY)
        self.assertNotEqual(t.yesterday_gmt(NOW - t.ONEDAY), YESTERDAY)

    def test_yesterday(self):
        local_today = YESTERDAY + TIME_ZONE_SHIFT
        self.assertEqual(t.yesterday(), local_today)
        self.assertEqual(t.yesterday(NOW + t.ONEHOUR), local_today)
        self.assertNotEqual(t.yesterday(NOW - t.ONEDAY), local_today)

    def test_day(self):
        self.assertEqual(t.first_second_of_day(time.localtime(NOW)),
                         TODAY + TIME_ZONE_SHIFT)
        self.assertEqual(t.first_second_of_day_gmt(time.gmtime(NOW)), TODAY)

    def test_month(self):
        TIME_CASE = {-15: (2009, 6, 1, 0, 0, 0, 0, 152, 0),
                     -10: (2009, 11, 1, 0, 0, 0, 6, 305, 0),
                      -5: (2010, 4, 1, 0, 0, 0, 3, 91, 0),
                       0: (2010, 9, 1, 0, 0, 0, 2, 244, 0),
                       5: (2011, 2, 1, 0, 0, 0, 1, 32, 0)}

        for offset in range(-15, 6, 5):
            self.assertEquals(time.gmtime(t.first_second_of_month_gmt(
                                                time.gmtime(NOW),
                                                offset)),
                              TIME_CASE[offset])

    def test_add_years(self):
        self.assertEqual(t.add_years(NOW, 1), NOW + t.ONEYEAR)

        # It's because of a leap year.
        self.assertAlmostEqual(t.add_years(NOW, 5), NOW + t.ONEYEAR * 5, -6)
        self.assertAlmostEqual(t.add_years(NOW, -5), NOW - t.ONEYEAR * 5, -6)

        self.assertEqual(t.add_time(NOW, 1, 'y'), NOW + t.ONEYEAR)
        self.assertNotEqual(t.add_time(NOW, 1, 'y'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'y'), NOW)

    def test_add_month(self):
        self.assertEqual(t.add_months(NOW, 1), NOW + t.ONEMONTH)

        # We assume that there're 30 days in a month.
        self.assertAlmostEqual(t.add_months(NOW, 5),
                               NOW + t.ONEMONTH * 5, -6)
        self.assertAlmostEqual(t.add_months(NOW, -5),
                               NOW - t.ONEMONTH * 5, -6)
        self.assertAlmostEqual(t.add_months(NOW, 10),
                               NOW + t.ONEMONTH * 10, -6)
        self.assertAlmostEqual(t.add_months(NOW, -10),
                               NOW - t.ONEMONTH * 10, -6)

        self.assertEqual(t.add_time(NOW, 1, 'M'), NOW + t.ONEMONTH)
        self.assertNotEqual(t.add_time(NOW, 1, 'M'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'M'), NOW)

    def test_add_months_start_time(self):
        f = '%Y-%m-%d'

        r1 = t.add_months(time.mktime(time.strptime('1970-01-01', f)), 1)
        r2 = time.mktime(time.strptime('1970-02-01', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1970-01-01', f)), 2)
        r2 = time.mktime(time.strptime('1970-03-01', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1970-01-01', f)), 2,
                          time.mktime(time.strptime('1970-01-01', f)))
        r2 = time.mktime(time.strptime('1970-03-01', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1970-01-01', f)), 2,
                          time.mktime(time.strptime('1970-01-31', f)))
        r2 = time.mktime(time.strptime('1970-03-31', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1970-01-31', f)), 1,
                          time.mktime(time.strptime('1970-01-31', f)))
        r2 = time.mktime(time.strptime('1970-02-28', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1972-01-31', f)), 1,
                          time.mktime(time.strptime('1972-01-31', f)))
        r2 = time.mktime(time.strptime('1972-02-29', f))
        self.assertEqual(r1, r2)

        r1 = t.add_months(time.mktime(time.strptime('1972-02-29', f)), 1,
                          time.mktime(time.strptime('1972-01-31', f)))
        r2 = time.mktime(time.strptime('1972-03-31', f))
        self.assertEqual(r1, r2)

    def test_add_years_start_time(self):
        f = '%Y-%m-%d'

        r1 = t.add_years(time.mktime(time.strptime('1970-01-01', f)), 1)
        r2 = time.mktime(time.strptime('1971-01-01', f))
        self.assertEqual(r1, r2)

        r1 = t.add_years(time.mktime(time.strptime('1970-02-28', f)), 2,
                         time.mktime(time.strptime('1970-01-31', f)))
        r2 = time.mktime(time.strptime('1972-02-29', f))
        self.assertEqual(r1, r2)

        r1 = t.add_years(time.mktime(time.strptime('1972-02-29', f)), -2)
        r2 = time.mktime(time.strptime('1970-02-28', f))
        self.assertEqual(r1, r2)

    def test_add_weeks(self):
        self.assertEqual(t.add_weeks(NOW, 1), NOW + t.ONEWEEK)
        self.assertEqual(t.add_weeks(NOW, 5), NOW + t.ONEWEEK * 5)
        self.assertEqual(t.add_weeks(NOW, -5), NOW - t.ONEWEEK * 5)
        self.assertEqual(t.add_time(NOW, 1, 'w'), NOW + t.ONEWEEK)
        self.assertNotEqual(t.add_time(NOW, 1, 'w'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'w'), NOW)

    def test_add_days(self):
        self.assertEqual(t.add_days(NOW, 1), NOW + t.ONEDAY)
        self.assertEqual(t.add_days(NOW, 5), NOW + t.ONEDAY * 5)
        self.assertEqual(t.add_days(NOW, -5), NOW - t.ONEDAY * 5)
        self.assertEqual(t.add_time(NOW, 1, 'd'), NOW + t.ONEDAY)
        self.assertNotEqual(t.add_time(NOW, 1, 'd'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'd'), NOW)

    def test_add_hours(self):
        self.assertEqual(t.add_hours(NOW, 1), NOW + t.ONEHOUR)
        self.assertEqual(t.add_hours(NOW, 5), NOW + t.ONEHOUR * 5)
        self.assertEqual(t.add_hours(NOW, -5), NOW - t.ONEHOUR * 5)
        self.assertEqual(t.add_time(NOW, 1, 'h'), NOW + t.ONEHOUR)
        self.assertNotEqual(t.add_time(NOW, 1, 'h'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'h'), NOW)

    def test_add_minutes(self):
        self.assertEqual(t.add_minutes(NOW, 1), NOW + t.ONEMINUTE)
        self.assertEqual(t.add_minutes(NOW, 5), NOW + t.ONEMINUTE * 5)
        self.assertEqual(t.add_minutes(NOW, -5), NOW - t.ONEMINUTE * 5)
        self.assertEqual(t.add_time(NOW, 1, 'm'), NOW + t.ONEMINUTE)
        self.assertNotEqual(t.add_time(NOW, 1, 'm'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 'm'), NOW)

    def test_add_seconds(self):
        self.assertEqual(t.add_seconds(NOW, 1), NOW + 1)
        self.assertEqual(t.add_seconds(NOW, 5), NOW + 5)
        self.assertEqual(t.add_seconds(NOW, -5), NOW - 5)
        self.assertEqual(t.add_time(NOW, 1, 's'), NOW + 1)
        self.assertNotEqual(t.add_time(NOW, 1, 's'), NOW)
        self.assertEqual(t.add_time(NOW, 0, 's'), NOW)

    def test_add_time_raise(self):
        self.assertRaises(Exception, t.add_time, NOW, '1', 's')
        self.assertRaises(Exception, t.add_time, str(NOW), 1, 's')
        self.assertRaises(ValueError, t.add_time, NOW, 1, 'wrong')

    def test_timestamp_to_datestring_raise(self):
        self.assertRaises(ValueError, t.timestamp_to_datestring,
                          NOW, zone='wrong_timezone')

    def test_timestamp_to_datestring(self):
        self.assertEqual(t.timestamp_to_datestring(YESTERDAY),
                         '2010-08-31 00:00:00 UTC')
        self.assertEqual(t.timestamp_to_datestring(YESTERDAY, zone='EEST'),
                         '2010-08-31 03:00:00 EEST')
        self.assertEqual(t.timestamp_to_datestring(YESTERDAY, zone='PST'),
                         '2010-08-30 16:00:00 PST')
        self.assertEqual(t.timestamp_to_datestring(0),
                         '1970-01-01 00:00:00 UTC')

    def test_timestamp_to_datestring_zones(self):
        self.assertEqual(t.timestamp_to_datestring(NOW, '%Z'), 'UTC')
        self.assertEqual(t.timestamp_to_datestring(NOW, '%Z', 'PST'), 'PST')
        self.assertEqual(t.timestamp_to_datestring(NOW, '%Z', 'PDT'), 'PDT')
        self.assertEqual(t.timestamp_to_datestring(NOW, '%Z%z', 'EEST'),
                         'EEST+0300')
        self.assertEqual(t.timestamp_to_datestring(0, '%Z%z'), 'UTC+0000')

    def test_datestring_to_timestamp_raise(self):
        self.assertRaises(ValueError, t.datestring_to_timestamp,
                          '1970-01-01 00:00:00 UTC', 'wrong format')
        self.assertRaises(ValueError, t.datestring_to_timestamp,
                          '1970-01-01 00:00:00', default_tz='wrong zone')

    def test_datestring_to_timestamp(self):
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 00:00:00 UTC'),
                         0)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 00:00:01 UTC'),
                         1)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 02:00:00 EET'),
                                                   0)
        self.assertEqual(t.datestring_to_timestamp('2010-08-31 00:00:00 UTC'),
                         YESTERDAY)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 00:00:00 PST'),
                         28800)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 00:00:00 EET'),
                         - 7200)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01', '%Y-%m-%d',
                                                   'EET'), -7200)

    def test_datestring_to_timestamp_formats(self):
        no_tz = '%Y-%m-%d %H:%M:%S'
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 00:00:01',
                                                   no_tz), 1)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 02:00:00',
                                                   no_tz, 'EET'), 0)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 02:00:00',
                                                   no_tz), 7200)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 02:00:00 EET',
                                                   no_tz), 7200)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 02:00:00 EET',
                                                   no_tz, 'EEST'),
                         t.datestring_to_timestamp('1970-01-01 02:00:00 EEST'))
        self.assertEqual(t.datestring_to_timestamp('1970-01-01', '%Y-%m-%d'),
                         0)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 1',
                                                   '%Y-%m-%d %H'), 3600)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01 1',
                                                   '%Y-%m-%d %S'), 1)
        self.assertEqual(t.datestring_to_timestamp('2010-08-25 UTC',
                                                   '%Y-%m-%d %Z'), WEEK_AGO)
        self.assertEqual(t.datestring_to_timestamp('1970-01-01', '%Y-%m-%d'),
                         0)

    def test_get_time_intervals(self):
        self.assertEqual(t.get_time_intervals(0, 2678400 + 1, 1, 'm'),
                         [[0, 2678400], [2678400, 2678401]])
        self.assertEqual(t.get_time_intervals(0, 2678400 + 1, 2, 'm'),
                         [[0, 2678401]])
        self.assertEqual(t.get_time_intervals(0, 3600 + 1, 1, 'h'),
                         [[0, 3600], [3600, 3601]])
        self.assertEqual(t.get_time_intervals(0, 3600 + 1, 2, 'h'),
                         [[0, 3601]])
        self.assertEqual(t.get_time_intervals(0, -3600, 1, 'h'), [])
        self.assertEqual(t.get_time_intervals(0, -3600, -1, 'h'), [[-3600, 0]])
        self.assertEqual(t.get_time_intervals(0, 3600, 1, 'h'), [[0, 3600]])
        self.assertEqual(t.get_time_intervals(3600, 0, 1, 'h'), [])
        self.assertRaises(ValueError, t.get_time_intervals, 0, 3600, 1, 'H')
        self.assertRaises(ValueError, t.get_time_intervals, 0, 3600, 1,
                          'wrong_key')

    def test_get_time_intervals_tricky_periods(self):

        s = time.mktime(time.strptime('1970-02-01', '%Y-%m-%d'))

        self.assertEqual(t.get_time_intervals(s, s + t.ONEDAY * 28, 1, 'm'),
                         [[2707200.0, 5126400.0]])
        self.assertEqual(t.get_time_intervals(s, s + t.ONEDAY * 29, 1, 'm'),
                         [[2707200.0, 5126400.0], [5126400.0, 5212800.0]])

        s = time.mktime(time.strptime('1970-02-28', '%Y-%m-%d'))

        self.assertEqual(t.get_time_intervals(s, s + t.ONEDAY * 28, 1, 'm'),
                         [[5040000.0, 7459200.0]])
        self.assertEqual(t.get_time_intervals(s, s + t.ONEDAY * 29, 1, 'm'),
                         [[5040000.0, 7459200.0], [7459200.0, 7545600.0]])

        s = time.mktime(time.strptime('1970-02-01', '%Y-%m-%d'))
        e = time.mktime(time.strptime('1970-05-01', '%Y-%m-%d'))
        self.assertEqual(t.get_time_intervals(s, e, 1, 'm'),
                         [[2707200.0, 5126400.0], # 1970-02-01 - 1970-03-01
                          [5126400.0, 7804800.0], # 1970-03-01 - 1970-04-01
                          [7804800.0, 10393200.0]]) # 1970-04-01 - 1970-05-01

        s = time.mktime(time.strptime('1970-02-01', '%Y-%m-%d'))
        e = time.mktime(time.strptime('1970-05-01', '%Y-%m-%d'))
        self.assertEqual(t.get_time_intervals(s, e, 3, 'm'),
                         [[2707200.0, 10393200.0]]) # 1970-02-01 - 1970-05-01

        s = time.mktime(time.strptime('1971-01-31', '%Y-%m-%d'))
        e = time.mktime(time.strptime('1971-02-28', '%Y-%m-%d'))
        self.assertEqual(t.get_time_intervals(s, e + 1, 1, 'm'),
                         [[34156800.0, 36576000.0], [36576000.0, 36576001.0]])

        s = time.mktime(time.strptime('1972-02-29', '%Y-%m-%d'))
        e = time.mktime(time.strptime('1970-02-28', '%Y-%m-%d'))
        self.assertEqual(t.get_time_intervals(e, s, 1, 'y'),
                         [[5040000.0, 36576000.0], # 1970-02-28 - 1971-02-28
                          [36576000.0, 68112000.0], # 1971-02-28 - 1972-02-28
                          [68112000.0, 68198400.0]]) # 1972-02-28 - 1972-02-29

        self.assertEqual(t.get_time_intervals(s, e, -1, 'y'),
                         [[36576000.0, 68198400.0], # 1971-02-28 - 1972-02-29
                          [5040000.0, 36576000.0]]) # 1970-02-28 - 1971-02-28

    def test_datetime_to_timestamp(self):
        dt = datetime.datetime(1970, 1, 1)
        self.assertEqual(t.datetime_to_timestamp(dt), 0)
        dt = datetime.datetime(1970, 1, 1, 1)
        self.assertEqual(t.datetime_to_timestamp(dt), 3600)

    def test_datestring_to_datetime(self):
        str_dt1 = str(t.datestring_to_datetime('1972-02-29', '%Y-%m-%d'))
        str_dt2 = str(datetime.datetime(1972, 2, 29, 0, 0, 0, 0, t.TZ('UTC')))
        self.assertEqual(str_dt1, str_dt2)

        str_dt1 = str(t.datestring_to_datetime('1972-02-29', '%Y-%m-%d',
                                               'PST'))
        str_dt2 = str(datetime.datetime(1972, 2, 29, 0, 0, 0, 0, t.TZ('PST')))
        self.assertEqual(str_dt1, str_dt2)


if __name__ == "__main__":
    unittest2.main()
