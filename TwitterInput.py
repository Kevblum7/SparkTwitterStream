import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json



class TweetsStream(tweepy.StreamListener):

  def __init__(self, client_socket ):
      self.client_socket = client_socket


  def on_data(self, packet):
      try:
          data = json.loads( packet )
          print(data['text'].encode('utf-8'))
          self.client_socket.send( data['text'].encode('utf-8'))
          return True
          
      except BaseException as e:
          print("{ERROR!:  %s" % str(e) + " }")
      return True

  def on_error(self, status):
      print(status)
      return True

def tweetGetter(client_socket, consumer_key, consumer_secret, access_token, access_token_secret ):

  authentication = OAuthHandler(consumer_key, consumer_secret)
  
  authentication.set_access_token(access_token, access_token_secret)
  
  incoming_tweets = Stream(authentication, TweetsStream(client_socket))
  
  incoming_tweets.filter(track=['president'])

if __name__ == "__main__":
    
  # authentication keys
  
  consumer_key = ""
  consumer_secret = ""
  access_token = ""
  access_token_secret = ""
  
  port_number = 5556
  local_host = "127.0.0.1"

  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  
  # connecting local host and port for listening
  
  sock.bind((local_host, port_number))

  print("Stream Listener Port: %s" % str(port_number))

  sock.listen(5)
  
  client, addr = sock.accept()

  print("Connecting to address: " + str(addr))

  tweetGetter(client, consumer_key, consumer_secret, access_token, access_token_secret)
