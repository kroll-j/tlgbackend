#!/opt/ts/python/2.7/bin/python
# task list generator - utility stuff for sending email
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def sendFriendlyBotMessage(recipient, text, attachmentText, attachmentSubtype):
    mailFrom= 'The Friendly Task List Robot <noreply@weyland-yutani.com>'
    mailTo= recipient
    msg= MIMEMultipart()
    msg['Subject'] = 'Task List'
    msg['From']= mailFrom
    msg['To']= mailTo
    
    msgText= MIMEText(text)
    msg.attach(msgText)
    
    attachment= MIMEText(attachmentText, attachmentSubtype, 'utf-8')
    if attachmentSubtype=='plain': fn= 'output.txt'
    else: fn= 'output.'+attachmentSubtype
    attachment.add_header('Content-Disposition', 'attachment', filename=fn)
    msg.attach(attachment)
    
    s = smtplib.SMTP('localhost')
    s.sendmail(mailFrom, [mailTo], msg.as_string())
    s.quit()
    
    

if __name__=='__main__':
    sendFriendlyBotMessage('junk-mailbox@gmx.de', 'the foo of the bar.\n', """<html><head><title>Foo Bar!</title></head><body>blah blah.</body></html>""", 'html')
    sys.exit(0)
    

    # the base message container
    msg= MIMEText("""Hello, World!
Foo, Bar, Baz.
End of message.
""")

    mailFrom= 'The Friendly Task List Robot <noreply@weyland-yutani.com>'
    mailTo= 'junk-mailbox@gmx.de'
    
    msg['Subject'] = 'The foo of the bar'
    msg['From'] = mailFrom
    msg['To'] = mailTo

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP('localhost')
    s.sendmail(mailFrom, [mailTo], msg.as_string())
    s.quit()
