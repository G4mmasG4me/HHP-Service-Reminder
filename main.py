import mysql.connector
import datetime
import smtplib

import schedule
import time
import csv
import os

from connect import mydb

import details
with open('email_template.html') as f:
  email_template = f.read()

mycursor = mydb.cursor(prepared=True)

subject = 'Book your upcoming boiler service!'


def send_email(smtp_server, receiver, name, address, upcoming_service):
  # generate email
  body = email_template.format(name=name, service_date=upcoming_service, address=address)
  msg = f'From: {details.sender}\nTo: {receiver}\nSubject: {subject}\nContent-Type: text/html;\n\n{body}'


  # tries to send email up to 5 times
  for i in range(5):
    try:
      smtp_server.sendmail(details.sender, receiver, msg)
      return True, None
    except Exception as e:
      pass
  return False, e
  


def open_smtp():
  for i in range(5): # try and connect up to 5 times
    try:
      smtp_server = smtplib.SMTP('smtp.office365.com', 587)
      smtp_server.ehlo() # Hostname to send for this command defaults to the fully qualified domain name of the local host.
      smtp_server.starttls() #Puts connection to SMTP server in TLS mode
      smtp_server.ehlo()

      smtp_server.login(details.sender, details.password)
      return smtp_server, True, None # server object, pass, error
    except Exception as e:
      pass
  return None, False, e # server object, pass, error


def close_smtp(smtp_server):
  smtp_server.quit()

day_notice = 14
sent_email_sql = 'INSERT INTO `sent_email` (customer_id) VALUES (%s)'
def main():
  sql = """ 
  SELECT customer.id, CONCAT_WS(" ", customer.title, customer.first_name, customer.last_name) AS name customer.email, CONCAT_WS(", ", address.line_1, address.line_2, address.postcode, address.city) AS address, MAX(service.service_date) AS last_service_date, (MAX(service.service_date) + INTERVAL 365 DAY) AS next_service_date, MAX(sent_email.created) AS last_sent_email 
  FROM service 
  LEFT JOIN sent_email ON service.customer_id = sent_email.customer_id 
  JOIN customer on customer.id = service.customer_id
  JOIN customer_address ON customer_address.customer_id = customer.id
  JOIN address ON address.id = customer_address.address_id
  GROUP by service.customer_id
  HAVING (CURDATE() BETWEEN next_service_date - INTERVAL %i DAY AND next_service_date) AND (last_sent_email IS NULL OR last_sent_email NOT BETWEEN next_service_date - INTERVAL %i DAY AND next_service_date)
  """
  values = (day_notice, day_notice)
  mycursor.execute(sql, values)
  result = mycursor.fetchall()
  if result:
    # connect to smtp server
    smtp_server, success, error = open_smtp()
    #  if connect, loop through mysql results
    if success:
      for row in result:
        # collect row data
        customer_id = row[0]
        name = row[1]
        email = row[2]
        address = row[3]
        previous_service = row[4]
        upcoming_service = row[5]
        last_email_sent = row[6]

        # send email
        success, error = send_email(smtp_server, name, email, address, upcoming_service)

        # execute 
        if success:
          mycursor.execute(sent_email_sql, customer_id)
        with open('email_logs.csv', 'a') as f:
          writer = csv.writer(f)
          time = datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%S')
          writer.writerow([time, customer_id, previous_service, upcoming_service, last_email_sent, error])
  with open('running_logs.csv') as f:
    writer = csv.writer(f)
    time = datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%S')
    writer.writerow([time, len(result), error])


def run():
  # create running logs csv if it doesn't exist
  if not os.path.exists('running_logs.csv'):
    with open('running_logs.csv', 'w') as f:
      writer = csv.writer(f)
      writer.writerow(['DateTime (UTC)', 'Emails To Be Sent', 'Error'])

  # create email logs csv if it doesnt' exist
  if not os.path.exists('email_logs.csv'):
    with open('email_logs.csv', 'w') as f:
      writer = csv.writer(f)
      writer.writerow(['DateTime (UTC)', 'Customer ID', 'Previous Service (UTC)', 'Upcoming Service (UTC)', 'Last Email Sent (UTC)', 'Error'])
    
  # runs schedule
  while True:
    schedule.run_pending()
    time.sleep(1)

smtp_server, success, error = open_smtp()
success, error = send_email(smtp_server, 'domhough@hotmail.co.uk', 'Mr Dominic Rollason-Hough', '101 Castlecroft Road, WV3 8BY, West Midlands', '14/02/2004')
if success:
  print('Success')
else:
  print(error)
quit()
schedule.every().day.at('09:00').do(main)
run()