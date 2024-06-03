import sys
import time
import pika

class Receiver:
    def __init__(self, name):
        self.name = name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='test')
        self.channel.queue_declare(queue='test_reply')
    
    def callback(self, ch, method, properties, body):
        self.consume(body.decode())
        self.channel.basic_publish(exchange='',
                                   routing_key='test_reply',
                                   body=f'{body},{self.name}')
    
    def receive(self):
        self.channel.basic_consume(queue='test',
                        on_message_callback=self.callback,
                        auto_ack=True)
        print("RECEIVING...")
        self.channel.start_consuming()
        print("DONE")
        
    def consume(self, body):
        sleep_time = int(body.split(',')[3])
        print(f"RECEIVER {self.name} got: {body}")
        
name = sys.argv[1]
receiver = Receiver(name)
receiver.receive()