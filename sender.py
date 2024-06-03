import random
import time
import pika

start = 0
send_time = 0

class Sender:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='test')
        self.channel.queue_declare(queue='test_reply')
        self.log = open('sender.log', 'w')
    
    def send(self, message):
        self.channel.basic_publish(exchange='',
                        routing_key='test',
                        body=message)
        print(f" [x] Sent {message}")
    
    def close(self):
        self.connection.close()
        
    def produce(self):
        for i in range(10000):
            self.send(f"M,{i},S,{random.randint(0, 10)}")
        send_time = time.time() - start
        
    
    def receive(self):
        self.channel.basic_consume(queue='test_reply',
                        on_message_callback=self.callback,
                        auto_ack=True)
        print("RECEIVING...")
        self.channel.start_consuming()
        print("DONE")
    
    def callback(self, ch, method, properties, body):
        body = body.decode()
        d = body.split(',')
        if d[1] == '9999':
            print(f"############# Receive Time: {time.time() - start}")
            print(f"############# Send Time: {send_time}")
        print(f" [x] Received {body}")
        self.log.write(f"{body} \n")
        self.log.flush()

sender = Sender()
start = time.time()
sender.produce()
start = time.time()
sender.receive()