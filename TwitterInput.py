import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json


# Set up your credentials
consumer_key    = "Ax79DI6gXdMjOCQyQ61v2Khbj"
consumer_secret = "DBSmfUL50FlhpVnePa373lOpBFffJjrGInXbSGG4T7Ze2RRQTl"
access_token    = "1418173405988376579-9g3SF5klnWOgd62nUfTfaqrQdzkJRA"
access_token_secret   = "q3jO28xbGDlvE6djCNeoSMQbtkosZYT7mBTv0O6wznMpw"


class TweetsListener(tweepy.StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print(msg['text'].encode('utf-8'))
          self.client_socket.send( msg['text'].encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['president'])

if __name__ == "__main__":
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 5556                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print("Received request from: " + str(addr))

  sendData(c)
