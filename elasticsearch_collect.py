from twisted.words.protocols import irc
from twisted.internet import reactor, protocol, task
import datetime, requests

from elasticsearch import Elasticsearch, helpers
from env_variables import *
from time import sleep

class DataHandler():

	def __init__(self):
		self.payload = []
		self.payload_threshold = 100

	def processTwitchMessage(self, message, user, channel, twitch_id):
		
		# ignore 1 character messages and messages longer than 50 characters (crude spam filter) 

		if (len(message) > 50) | (len(message) == 1):
			return

		channel_name = channel.split('#')[1]
		username = user.split('!')[0]
		time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

		self.payload.append((channel_name, username, message, time, twitch_id))

		print '[' + time + ']', len(self.payload), channel, username + ':', message

		if len(self.payload) > self.payload_threshold - 1:
			self.dump_payload()

	def dump_payload(self):

		cached_payload = self.payload[:]
		del self.payload[:]
		bulk_string_iter = [{'_index': msg[4], '_type':'message', 'time': msg[3], 'content': msg[2], 'username': msg[1], 'channel': msg[0]} for msg in cached_payload]

		helpers.bulk(es, bulk_string_iter)

		print 'Payload dumped!'


class ChannelListener(irc.IRCClient):

	nickname = NICKNAME
	password = PASSWORD
	running_streams = []
	channel_to_id = {}
	channel_to_starttime = {}

	def connectionMade(self):
		print 'connection made!'
		irc.IRCClient.connectionMade(self)
		self.data_handler = DataHandler()

	def connectionLost(self, reason):
		print 'connection lost!'
		irc.IRCClient.connectionLost(self, reason)

	def signedOn(self):
		"""Called when bot has succesfully signed on to server."""
		print 'Getting streams...'
		current_streams = self.getStreams()
		for stream_name in current_streams:
			self.running_streams.append(stream_name)
			self.join(stream_name)
		lc = task.LoopingCall(self.updateStreams)
		lc.start(30)

		print 'signed on, joining channel!'

	def joined(self, channel):
		"""This will get called when the bot joins the channel."""
        print 'joined'

	def privmsg(self, user, channel, msg):
		try:
			twitch_id = self.channel_to_id[channel.split('#')[1]]
			self.data_handler.processTwitchMessage(msg, user, channel, twitch_id)
		except Exception:
			pass

	def updateStreams(self):
		current_streams = self.getStreams()
		print "running streams: " + " ".join(self.running_streams)
		current_stream_set = set(current_streams)
		running_stream_set = set(self.running_streams)

		streams_to_join = current_stream_set - running_stream_set
		streams_to_leave = running_stream_set - current_stream_set

		print  "ONLINE STREAMERS: " + " ".join(current_streams)

		if len(streams_to_join) != 0:
			# join new channels
			for stream in streams_to_join:
				print "NOW COLLECTING FROM " + stream
				self.running_streams.append(stream)
				self.join(stream)

		if len(streams_to_leave) != 0:
			# leave old channels
			for stream in streams_to_leave:
				print stream + " HAS GONE OFFLINE - COLLECTION FINISHED"
				self.running_streams.remove(stream)
				del self.channel_to_id[stream]
				del self.channel_to_starttime[stream]
				self.leave(stream)

	def getStreams(self):
		# queries twitch api in order to find out which followed streams are currently online
		url_string = ACTIVE_STREAM_API_ENDPOINT
		r = requests.get(url_string)

		try:
			data = r.json()
		except Exception:
			data = []
			
		time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

		channel_array = []
		bulk_data_string = ''

		if 'streams' in data:

			for stream in data['streams']:
				stream_name = stream['channel']['name'].encode('utf-8')
				stream_start = stream['created_at'].encode('utf-8')

				# check to see if streamtime is defined or if start time is different (violated DRY principle here)
				if stream_name in self.channel_to_starttime:
					if self.channel_to_starttime[stream_name] != stream_start:
						self.channel_to_starttime[stream_name] = stream_start
						sleep(1)
						vod_id = requests.get('https://api.twitch.tv/kraken/channels/'+ stream_name +'/videos?limit=1&broadcasts=true').json()['videos'][0]['_id'].encode('utf-8')
						self.channel_to_id[stream_name] = vod_id
					else:
						pass

				else:
					self.channel_to_starttime[stream_name] = stream_start
					sleep(1)
					try: 
						vod_id = requests.get('https://api.twitch.tv/kraken/channels/'+ stream_name +'/videos?limit=1&broadcasts=true').json()['videos'][0]['_id'].encode('utf-8')
					except Exception:
						vod_id = stream_name 
					self.channel_to_id[stream_name] = vod_id

				channel_array.append(stream_name)

				# also dump viewer count to elasticsearch while stream metadata is in scope
				try:
					print 'Streamer', stream_name, 'has', stream['viewers'], 'viewers.'
					es.index(self.channel_to_id[stream_name], 'stats', {"viewers": str(stream['viewers']), "time": time })
				except Exception as e:
					print e
		
		return channel_array

class ChannelListenerFactory(protocol.ClientFactory):

	prot = ChannelListener()

	def buildProtocol(self, addr):
		self.prot.factory = self
		return self.prot

	def clientConnectionLost(self, connector, reason):
		"""If we get disconnected, reconnect to server."""
		print 'clientconnectionlost TRIGGERED'
		connector.connect()

	def clientConnectionFailed(self, connector, reason):
		print "connection failed:", reason
		reactor.stop()

if __name__ == '__main__':
 
	es = Elasticsearch()

 	f = ChannelListenerFactory()

	# connect factory protocol and application to this host and port
	reactor.connectTCP("irc.twitch.tv", 6667, f)
	
	# run bot
	reactor.run()

