"""Utility library for generating and sending e-mail messages via SMTP.

:Author: vburenin
:Version: $Id: //prod/main/_is/shared/python/mail/smtp.py#6 $
"""

import logging
import mimetypes
import os
import smtplib

from email import Encoders
from email.header import Header
from email.mime.audio import MIMEAudio
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEBase, MIMEMultipart
from email.mime.text import MIMEText

class Mailer(object):

    """SMTP mail util, that provide simple way to send e-mail messages."""

    def __init__(self, smtp_host):
        """Simple Mailer constructor.

        :param smtp_host: The smtp server that will be used to send e-mail
                          message.
        """
        self.__smtp_host = smtp_host
        self.__log = logging.getLogger('shared.mail.smtp.Mailer')

    def send_mail(self, address_from, address_to, body,
                  subject=None, headers=None,
                  attached_files=None):
        """Build and sends e-mail message.

        :param address_from: Sender e-mail address.
        :param address_to: Recipient e-mail address.
        :param body: message text body.
        :param subject: message subject.
        :param headers: Additional message headers.
        :param attached_files: A list of paths to files which need to be attached.
        """

        if headers is None:
            headers = {}

        if subject is not None:
            headers['Subject'] = subject

        # Generate full email message with headers.
        if attached_files:
            message = MIMEMultipart()
            message.attach(MIMEText(body))
            self.__attach_files_to_message(message, attached_files)
        else:
            message = MIMEText(body)

        message['From'] = address_from
        message['To'] = address_to

        for key, value in headers.items():
            message[key] = Header(value)

        message.epilogue = ''
        server = smtplib.SMTP(self.__smtp_host)
        self.__log.debug('Sending message...')
        server.sendmail(address_from, address_to.split(','), message.as_string())
        self.__log.debug('Done. Message is sent.')
        server.quit()

    def __attach_files_to_message(self, message, files_list):
        """Attaches a list of files to the message.

        :param message: An instance of e-mail message.
        :param file_list: A list of paths to files which need to be attached.
        """
        for filename in files_list:
            self.__attach_file_to_message(message, filename)

    def __attach_file_to_message(self, message, filename):
        """Attaches a file to the message.

        :param message: An instance of e-mail message.
        :param filename: A path to a file that should be attached.
        """

        # Guess the content type based on the file's extension.  Encoding
        # will be ignored, although we should check for simple things like
        # gzip'd or compressed files.

        ctype, encoding = mimetypes.guess_type(filename)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed),
            # so use a generic bag-of-bits type.
            ctype = 'application/octet-stream'

        maintype, subtype = ctype.split('/', 1)

        file_data = self.__read_attachment_file(filename)

        # Just exit if there is no data.
        if file_data is None:
            return

        if maintype == 'text':
            mime_obj = MIMEText(file_data, _subtype=subtype)
        elif maintype == 'image':
            mime_obj = MIMEImage(file_data, _subtype=subtype)
        elif maintype == 'audio':
            mime_obj = MIMEAudio(file_data, _subtype=subtype)
        else:
            mime_obj = MIMEBase(maintype, subtype)
            mime_obj.set_payload(file_data)
            # Encode the payload using Base64.
            Encoders.encode_base64(mime_obj)

        mime_obj.add_header('Content-Disposition', 'attachment',
                           filename=os.path.basename(filename))
        message.attach(mime_obj)

    def __read_attachment_file(self, filename):
        """Read attachment file from file system.

        :param filename: Full path to a file.
        :return: File data.
        """
        try:
            fhandler = open(filename)
            try:
                data = fhandler.read()
                return data
            finally:
                fhandler.close()
        except IOError:
            self.__log.warning('Can not read file: %s', filename)
            raise
