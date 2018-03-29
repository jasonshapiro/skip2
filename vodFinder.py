import math
import requests
import datetime
from dateutil import parser
import manage
from skiptwo.models import Vod, Channel
import time
from data_methods import *

def populateStream(channel):

	url_string = "https://api.twitch.tv/kraken/channels/" + channel + "/videos?limit=5&broadcasts=true&client_id=5msydrle7ivo8xjt42pg1v0axlk633j"
	r = requests.get(url_string)
	data = r.json()

	# get all videos ( change this maybe )
	for i in data['videos']:
		start_time = parser.parse(i['recorded_at']).replace(tzinfo=None)
		start_string = datetime_to_string(start_time, '')
		duration = int(i['length'])
		vod_url = i['url']
		twitch_id = i['_id']
		title = i['title']
		game = i['game']

		vods = Vod.objects(channel=channel)


		for existing_vod in vods:
			
			if i['status'] == "recording":
#				break
				pass

			if existing_vod.start_time == start_time:
				if existing_vod.duration != duration:
					print 'Old Duration:', existing_vod.duration
					print 'New Duration:', duration
					existing_vod.delete()
					avg_viewers = getAverageViewerCount(twitch_id, datetime_to_string(start_time, ''), duration)

					if (avg_viewers > 0) & (duration > 3600):
						new_vod = Vod(start_time=start_time, duration=duration, channel=channel, url=vod_url, twitch_id=twitch_id, avg_viewer_count=avg_viewers, title = title, game = game)
						new_vod.save()
						print 'VOD Saved: ', start_time, 'Title:', title, 'Duration:', duration, 'Viewers:', avg_viewers, channel, vod_url


				break

		else: 
			avg_viewers = getAverageViewerCount(twitch_id, datetime_to_string(start_time, ''), duration)
			print 'Vod ID:' + twitch_id + ' -- Viewers:' + str(avg_viewers), start_string

			if (avg_viewers > 0) & (duration > 3600):
				new_vod = Vod(start_time = start_time, duration = duration, channel = channel, url = vod_url, twitch_id=twitch_id, avg_viewer_count=avg_viewers, title = title, game = game)
				new_vod.save()
				print 'VOD Saved: ', start_time, 'Title:', title, 'Duration:', duration, 'Viewers:', avg_viewers, channel, vod_url


def getImageURL():

	url_string = "elasticsearch_url"

	msg, sec = stringQueryElasticsearch()


r = requests.get('https://api.twitch.tv/kraken/users/jigglegiggle/follows/channels?limit=100&client_id=5msydrle7ivo8xjt42pg1v0axlk633j')
channel_data = r.json()
channels = [stream['channel']['name'].encode('utf-8') for stream in channel_data['follows']]

for channel in channels:
	time.sleep(1.5)
	channel_object = Channel.objects(name=channel)
	image_url = requests.get('https://api.twitch.tv/kraken/channels/' + channel).json()['logo'].encode('utf-8')
	new_channel_object = Channel(name=channel, image=image_url)
	new_channel_object.save()
	print 'Finding vods for: ' + channel
	populateStream(channel)
