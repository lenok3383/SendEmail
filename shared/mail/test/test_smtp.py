"""Unit tests for logging package.

:Status: $Id: //prod/main/_is/shared/python/mail/test/test_smtp.py#8 $
:Authors: vburenin
"""

import __builtin__
import StringIO
import logging
import re
import shared.mail.smtp
import smtplib
import unittest2

from shared.testing.vmock import matchers
from shared.testing.vmock.mockcontrol import MockControl

TEST_APP_NAME = 'smtp_test'

SMTP_HOST = 'localhost'
ADDR_FROM = 'test@ukr.net'
ADDR_TO = 'someone@gmail.com'

PLAIN_MESSAGE_WITHOUT_SUBJECT = """Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
From: test@ukr.net
To: someone@gmail.com

test msg"""


PLAIN_MESSAGE_MULTIPLE_RECIPIENTS = """Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
From: test@ukr.net
To: someone@gmail.com,test@ukr.net

test msg"""


PLAIN_MESSAGE_WITH_SUBJECT = """Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
From: test@ukr.net
To: someone@gmail.com
Subject: test subj

test msg"""


PLAIN_MESSAGE_WITH_SUBJECT_AND_HEADER = """Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
From: test@ukr.net
To: someone@gmail.com
Subject: test subj
X-Test: test

test msg"""


MESSAGE_WITH_ATTACHMENTS = """Content-Type: multipart/mixed; boundary="==123=="
MIME-Version: 1.0
From: test@ukr.net
To: someone@gmail.com

--==123==
Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

test msg
--==123==
Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="1.txt"

1.txt
--==123==
Content-Type: audio/mpeg
MIME-Version: 1.0
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="1.mp3"

MS5tcDM=
--==123==
Content-Type: image/jpeg
MIME-Version: 1.0
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="1.jpg"

MS5qcGc=
--==123==
Content-Type: application/octet-stream
MIME-Version: 1.0
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="1.dat"

MS5kYXQ=
--==123==--
"""


MESSAGE_WITH_IGNORED_BAD_FILE = """Content-Type: multipart/mixed; boundary="==123=="
MIME-Version: 1.0
From: test@ukr.net
To: someone@gmail.com

--==123==
Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

test msg
--==123==--
"""


# Setup logging.
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestMailer(unittest2.TestCase):

    def setUp(self):
        self.mc = MockControl()
        self.smtp_mock = self.mc.mock_class(smtplib.SMTP, display_name='smtp')
        self.mc.mock_constructor(smtplib, 'SMTP')(SMTP_HOST)\
            .returns(self.smtp_mock).anyorder().anytimes()

    def tearDown(self):
        self.mc.tear_down()

    def test_plain_message_without_subject(self):
        self.smtp_mock.sendmail(ADDR_FROM, [ADDR_TO],
                                 PLAIN_MESSAGE_WITHOUT_SUBJECT)
        self.smtp_mock.quit()
        self.mc.replay()
        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        mailer.send_mail(ADDR_FROM, ADDR_TO, 'test msg')
        self.mc.verify()

    def test_plain_message_multiple_recipients(self):
        self.smtp_mock.sendmail(ADDR_FROM, [ADDR_TO, ADDR_FROM],
                                PLAIN_MESSAGE_MULTIPLE_RECIPIENTS)
        self.smtp_mock.quit()
        self.mc.replay()
        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        mailer.send_mail(ADDR_FROM, '%s,%s' % (ADDR_TO, ADDR_FROM),
                         'test msg')
        self.mc.verify()

    def test_plain_message_with_subject(self):
        self.smtp_mock.sendmail(ADDR_FROM, [ADDR_TO],
                                PLAIN_MESSAGE_WITH_SUBJECT)
        self.smtp_mock.quit()

        self.mc.replay()
        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        mailer.send_mail(ADDR_FROM, ADDR_TO, 'test msg', 'test subj')
        self.mc.verify()

    def test_plain_message_with_subject_and_header(self):
        self.smtp_mock.sendmail(ADDR_FROM, [ADDR_TO],
                                PLAIN_MESSAGE_WITH_SUBJECT_AND_HEADER)
        self.smtp_mock.quit()

        self.mc.replay()
        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        mailer.send_mail(ADDR_FROM, ADDR_TO, 'test msg', 'test subj',
                         {'X-Test': 'test'})
        self.mc.verify()

    def test_message_with_files(self):
        orig_open = __builtin__.open
        def open_does(*fn):
            if fn[0] in ('1.txt', '1.mp3', '1.jpg', '1.dat'):
                return StringIO.StringIO(fn[0])
            else:
                return orig_open(*fn)

        def msg_matcher(msg):
            msg = re.sub('=+[0-9]+==', '==123==', msg)
            return msg == MESSAGE_WITH_ATTACHMENTS

        __builtin__.open = open_does

        self.smtp_mock.sendmail(ADDR_FROM, [ADDR_TO],
                                matchers.CustomMatcher(msg_matcher))
        self.smtp_mock.quit()
        self.mc.replay()

        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        mailer.send_mail(ADDR_FROM, ADDR_TO, 'test msg',
                         attached_files=['1.txt', '1.mp3', '1.jpg', '1.dat'])
        __builtin__.open = open_does
        self.mc.verify()

    def test_message_with_bad_files(self):
        fname = 'unexsiting_file.txt'
        self.mc.replay()
        mailer = shared.mail.smtp.Mailer(SMTP_HOST)
        self.assertRaises(IOError, mailer.send_mail, [ADDR_FROM], ADDR_TO,
                          'test msg', attached_files=[fname])
        self.mc.verify()


if __name__ == '__main__':
    unittest2.main()
