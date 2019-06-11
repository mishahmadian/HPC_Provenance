#!//usr/bin/python36
import pika
import os

#url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
#params = pika.URLParameters(url)
#params.socket_timeout = 5

credentials = pika.PlainCredentials('monitoring', 'Lu5t3r')
params = pika.ConnectionParameters('localhost',
                                       5672,
                                       '/monitoring',
                                       credentials)
#connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='jobstat', exchange_type='direct')
channel.queue_declare(queue='jobstat')
channel.queue_bind(exchange='jobstat', queue='jobstat')

def callback(ch, method, properties, body):
    print(" [=>] Received %r" %body)

channel.basic_consume(callback, queue='jobstat', no_ack=True)

print(" [*] Waiting for message. CTRL+C to exit")

channel.start_consuming()
