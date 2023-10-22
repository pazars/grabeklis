import utils

from datetime import datetime, timedelta


class TestParseDatetime:
    def test_standard_date(self):
        trial = "5. jūnijs, 2013, 13:00"
        correct = datetime(2013, 6, 5, 13, 0)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_this_year(self):
        trial = "14. novembris, 2011, 17:29"
        correct = datetime(2011, 11, 14, 17, 29)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_yesterday(self):
        trial = "Vakar, 19:54"

        now = datetime.now()
        yesterday = datetime.now() - timedelta(days=1)

        year = yesterday.year
        month = yesterday.month
        day = yesterday.day

        correct = datetime(year, month, day, 19, 54)
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_single_digit_h(self):
        trial = "14. novembris, 2011, 7:23"
        correct = datetime(2011, 11, 14, 7, 23)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_single_digit_min(self):
        trial = "14. novembris, 2011, 15:3"
        correct = datetime(2011, 11, 14, 15, 3)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_single_digit_both(self):
        trial = "14. novembris, 2011, 1:1"
        correct = datetime(2011, 11, 14, 1, 1)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_0_digit_both(self):
        trial = "14. novembris, 2011, 01:01"
        correct = datetime(2011, 11, 14, 1, 1)

        now = datetime.now()
        answer = utils.parse_datetime(trial, now)

        assert answer == correct

    def test_today(self):
        trial = "Šodien, 8:30"

        now = datetime.now()

        year = now.year
        month = now.month
        day = now.day

        correct = datetime(year, month, day, 8, 30)
        answer = utils.parse_datetime(trial, now)

        assert answer == correct
