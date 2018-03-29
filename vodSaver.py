import sys, requests, datetime
from data_methods import *
from env_variables import *

sys.path.append('/')

from skiptwo.models import Vod, Channel, ProtoVod
from mongoengine import Q
from time import sleep

# we have to query the channel/video API endpoint to get a broadcast's vod id
def saveVod(channel, start_time, proto_vod):
	sleep(3)
	constructChannel(channel)

	past_broadcasts_endpoint = "https://api.twitch.tv/kraken/channels/" + channel + "/videos?limit=5&broadcasts=true&client_id=5msydrle7ivo8xjt42pg1v0axlk633j"
	r = requests.get(past_broadcasts_endpoint)
	past_broadcasts = r.json()
	
	print 'Looking for ' + channel + ' VOD matches...' 

	for broadcast in past_broadcasts['videos']:

		if broadcast['status'] == 'recording':
#			break
			pass

		twitch_id = broadcast['_id']
		# there's usually a small (seconds) delay between when a streamer starts a broadcast, and when a vod starts recording
		recording_start_time = string_to_datetime(broadcast['recorded_at'])
		time_difference = recording_start_time - start_time

		print datetime_to_string(start_time, '')[:-3] + ' -- ' + channel + ' / Difference: ' + str(time_difference.total_seconds()) + 's'

		if abs(time_difference.total_seconds()) < 100:

			existing_vods = Vod.objects(twitch_id=twitch_id)

			duration = int(broadcast['length'])
			vod_url = broadcast['url']
			title = broadcast['title']
			game = broadcast['game']
			avg_viewers = getAverageViewerCount(twitch_id, datetime_to_string(recording_start_time, ''), duration)
			print 'Match Found! Processing:', title, 'from', channel, 'with', avg_viewers, 'viewers and is', duration, 'seconds long'

			if (avg_viewers > 0) & (duration > 3600):
				existing_vods.delete()
				new_vod = Vod(start_time=recording_start_time, duration=duration, channel=channel, url=vod_url, twitch_id=twitch_id, avg_viewer_count=avg_viewers, title = title, game=game)
				new_vod.save()
				print  '[' + datetime_to_string(datetime.datetime.now(),'') + ']', 'VOD Saved: ', start_time, 'Title:', title, '\nDuration:', duration, 'Viewers:', avg_viewers, channel, vod_url
			proto_vod.delete()
			

def constructChannel(channel):
	channel_object = Channel.objects(name=channel)
	if len(channel_object) == 0:
		api_data = requests.get('https://api.twitch.tv/kraken/channels/' + channel + '?client_id=5msydrle7ivo8xjt42pg1v0axlk633j').json()
		image_url = api_data['logo'].encode('utf-8')
		channel_language = api_data['language']
		new_channel_object = Channel(name=channel, image=image_url, language=channel_language)
		new_channel_object.save()


def updateFollowedChannels():
	all_streams_api_endpoint = 'https://api.twitch.tv/kraken/streams?client_id=5msydrle7ivo8xjt42pg1v0axlk633j'

	r = requests.get(all_streams_api_endpoint)
	data = r.json()

	for stream in data['streams']:
		if stream['viewers'] > 10000:
			channel = stream['channel']['name']
			existing_channel = Channel.objects(name='channel')
			if len(existing_channel) > 0:
				pass
			else:
				constructChannel(channel)
				r.post('')


#oauth:64qs9b6sys4gi4wz4oklime0dy0hbz

#try:

active_stream_endpoint = 'https://api.twitch.tv/kraken/streams/followed?oauth_token=tzmpierc6pikz4zbnseu4lm5bigoo6&scope=user_read'

r = requests.get(active_stream_endpoint)
active_streams = r.json()

stream_cache = []

for stream_object in active_streams['streams']:

	started_at_timestring = stream_object['created_at']
	started_at_datetime = string_to_datetime(started_at_timestring)
	channel = stream_object['channel']['name']
	query = ProtoVod.objects(Q(channel=channel) & Q(start_time=started_at_datetime))

	if len(query) == 0:
		proto_vod = ProtoVod(channel=channel, start_time=started_at_datetime)
		proto_vod.save()

	stream_cache.append((channel, started_at_datetime))

print '[' + datetime_to_string(datetime.datetime.now(),'') + '] Active Streams:', stream_cache

old_proto_vods = ProtoVod.objects(start_time__lte = datetime.datetime.utcnow()-datetime.timedelta(days=2))
old_proto_vods.delete()

stored_proto_vods = ProtoVod.objects()

proto_vod_cache = []

for proto_vod in stored_proto_vods:
	proto_vod_cache.append((proto_vod.channel, proto_vod.start_time))

print '[' + datetime_to_string(datetime.datetime.now(),'') + '] Unsaved Proto VoDs: \n', '\n'.join([datetime_to_string(vod[1],'')[:-3] + ' -- ' + vod[0] for vod in proto_vod_cache])

unsaved_proto_vods = set(proto_vod_cache) - set(stream_cache)

for i, vod in enumerate(unsaved_proto_vods):
	saveVod(vod[0], vod[1], ProtoVod.objects(Q(channel=vod[0]) & Q(start_time=vod[1])))

#except Exception as e:
#
#	print '[' + datetime_to_string(datetime.datetime.now(),'') + ']\n' + e
