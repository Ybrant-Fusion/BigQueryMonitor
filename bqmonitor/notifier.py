#!/usr/bin/env python

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


class SendMail(object):

    def __init__(self, smtpServer, smtpPort, smtpUsername, smtpPassword, mailSender, recipientTO, recipientCC, mailSubject):

        self.server       = str(smtpServer)
        self.port         = str(smtpPort)
        self.username     = str(smtpUsername)
        self.password     = str(smtpPassword)
        self.sender       = str(mailSender)
        self.subject      = str(mailSubject)
        self.recipient_to = recipientTO
        self.recipient_cc = recipientCC
        self.recipients   = []

        for recipient in recipientTO:
            self.recipients.append(recipient)

        for recipient in recipientCC:
            self.recipients.append(recipient)

    def send(self, content):

        msg = self.prepare(content)
        session = smtplib.SMTP(self.server, self.port)
        session.ehlo()
        session.starttls()
        session.ehlo()
        session.login(self.username, self.password)
        session.sendmail(self.sender, self.recipients, msg.as_string())
        session.close()

    def prepare(self, content):

        # Create message container.
        msg            = MIMEMultipart('alternative')
        msg['Subject'] = self.subject
        msg['From']    = self.sender
        msg['To']      = ','.join(self.recipient_to)
        msg['Cc']      = ','.join(self.recipient_cc)

        # Create the body of the message (an HTML version).
        html = '''\
        <html>
          <head></head>
          <body>
            The following jobs completed and was unsuccessful:
            <p>{content}</p>
          </body>
        </html>
        '''.format(content=content)

        # Record the MIME types.
        body = MIMEText(html, 'html')

        # Attach the body into message container.
        msg.attach(body)
        return msg
