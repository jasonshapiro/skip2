from flask import Blueprint, request, redirect, render_template, url_for
from flask.views import MethodView
from skiptwo.models import *
from skiptwo import cache
from data_methods import *
import requests, json, datetime
from dateutil import parser
from mongoengine import Q
from datetime import datetime, timedelta


twitch_vods = Blueprint('twitch_vods', __name__, template_folder='templates')


@cache.cached(timeout=60, key_prefix='order')
def generateVodOrder():

	last_week = datetime.utcnow() - timedelta(days=7)

	# get all vods in the last week
	vods = Vod.objects(Q(start_time__gte=last_week) & Q(avg_viewer_count__gte=1000) & Q(duration__gte=3600))

	vod_array = [{'id': vod.twitch_id, 'channel': vod.channel, 'title' : vod.title, 'avg_viewers': vod.avg_viewer_count, 'duration': vod.duration,'hours_since_broadcast': ((datetime.utcnow()-vod.start_time).total_seconds() - vod.duration)/3600} for vod in vods]

	for vod in vod_array:
		# recency factor buries results that are older than a day by 1.4x and 2 days by 2x (helps keep results fresh and helps smaller streams be more relevant)
		recency_factor = pow(vod['hours_since_broadcast']/12, 1.5)
		vod['display_score'] = vod['avg_viewers']*vod['duration']/recency_factor
	
	ordered_vods = [{'id': vod['id'], 'channel': vod['channel'], 'title' : vod['title'], 'viewers' : vod['avg_viewers'], 'recency' : vod['hours_since_broadcast']} for vod in sorted(vod_array, key=lambda vod: vod['display_score'], reverse=True)]

	return ordered_vods

@cache.memoize(timeout=60)
def generateJSONData(start_index, end_index):

	# for individual vods pass a string as start_index
	if type(start_index) is unicode:
		 
		 try:
		 	vod = Vod.objects(twitch_id=start_index)[0]
		 	vod_subset = [{'id': vod.twitch_id, 'channel': vod.channel, 'title' : vod.title, 'viewers': vod.avg_viewer_count, 'duration': vod.duration,'recency': ((datetime.utcnow()-vod.start_time).total_seconds() - vod.duration)/3600}]
		 except Exception as e:
		 	print 'No VoD found for', start_index, e

	else:
		ordered_vods = generateVodOrder()
		vod_subset = ordered_vods[start_index:end_index]

	descriptors = ['funny', 'plays']
	data = []

	for i, vod in enumerate(vod_subset):

			# unsafe line
			image_url = Channel.objects(name=vod['channel'])[0].image
			data.append({ 'twitch_id': vod['id'], 'channel': vod['channel'] , 'title': vod['title'], 'image_url': image_url, 'recency' : vod['recency'], 'viewers': vod['viewers'], 'data': [] })
			for descriptor in descriptors:
				# if elasticsearch / histogram API fails, don't assign any data
					try:

						vod_data = VodHistogramData.objects(Q(vod_id=vod['id']) & Q(descriptor=descriptor))
					
						if (len(vod_data) == 1):
							vod_data_array = vod_data[0].data
						else:
							r = requests.get("http://0.0.0.0/histogram/" + vod['id'] + "?descriptor=" + descriptor)
							vod_data_array = r.json()

						data[i]['data'].append({'descriptor': descriptor, 'data' : vod_data_array})

					except Exception as e:
						print str(e)

	return json.dumps(data)



#@cache.memoize(timeout=30)
#def generateChatStreamData(displacement=0)




class ChatStream(MethodView):

	def get(self):

		import rethinkdb as r

		conn = r.connect(host='localhost', db='realtime')

		t = datetime.utcnow() - timedelta(minutes=2)
		e = t + timedelta(seconds=30)

		start = r.time(t.year, t.month, t.day, t.hour, t.minute, t.second, "Z")
		end = r.time(e.year, e.month, e.day, e.hour, e.minute, e.second, "Z")

		cursor = r.table('ten_sec_chunks').between(start, end, index='start_time').run(conn)

		data = list(cursor)

		for chunk in data:
			chunk['start_time'] = chunk['start_time'].strftime("%Y-%m-%d %H:%M:%S")

		channels = data[0]['streams'].keys()

		data1 = {}

		for channel in channels:
			data1[channel] = ''

		for x in data:
			for y in channels:
				if y in x['streams']:
					for z in x['streams'][y]['corpus']:
						data1[y] += z


		print data1

		return json.dumps(data1)



class ChatStreamStats(MethodView):

	def get(self):

		import rethinkdb as r
		from nltk import word_tokenize, FreqDist, bigrams, trigrams

		conn = r.connect(host='localhost', db='realtime')

		t = datetime.utcnow() - timedelta(minutes=2)
		e = t + timedelta(seconds=30)

		start = r.time(t.year, t.month, t.day, t.hour, t.minute, t.second, "Z")
		end = r.time(e.year, e.month, e.day, e.hour, e.minute, e.second, "Z")

		cursor = r.table('ten_sec_chunks').between(start, end, index='start_time').run(conn)

		data = list(cursor)

		for chunk in data:
			chunk['start_time'] = chunk['start_time'].strftime("%Y-%m-%d %H:%M:%S")

		channels = data[0]['streams'].keys()

		data1 = {}
		data2 = {}

		for channel in channels:
			data1[channel] = {}
			data2[channel] = {}
			data1[channel]['joinedCorpus'] = ''
			data2[channel]['totalMsgCount'] = 0

# consolidate text corpus and message frequency

		for x in data:
			for y in channels:
				if y in x['streams']:
					for z in x['streams'][y]['corpus']:
						data1[y]['joinedCorpus'] += z
					data2[y]['totalMsgCount'] += x['streams'][y]['message_frequency']


		for streamName, streamObject in data1.iteritems():
			corpusArray = [word.lower() for word in streamObject['joinedCorpus'].encode('utf-8', 'replace').split()]
			data2[streamName]['freqData'] = FreqDist(corpusArray).pformat(3).strip('FreqDist(').strip(', ...})')
			data2[streamName]['freqData'] += '}'
#			data2[streamName]['bigramFreq'] = FreqDist(bigrams(corpusArray)).pformat(3).strip('FreqDist(').strip(', ...})')
#			data2[streamName]['bigramFreq'] += '}'
#			data2[streamName]['trigramFreq'] = FreqDist(trigrams(corpusArray)).pformat(3).strip('FreqDist(').strip(', ...})')
#			data2[streamName]['trigramFreq'] += '}'


		return json.dumps(data2)



class ChatStreamStats10(MethodView):

	def get(self):

		import rethinkdb as r
		from nltk import word_tokenize, FreqDist, bigrams, trigrams

		conn = r.connect(host='localhost', db='realtime')

# 10s buffer to ensure we aren't querying empty data

		t = datetime.utcnow() - timedelta(seconds=20)
		e = t + timedelta(seconds=10)

		start = r.time(t.year, t.month, t.day, t.hour, t.minute, t.second, "Z")
		end = r.time(e.year, e.month, e.day, e.hour, e.minute, e.second, "Z")

		cursor = r.table('ten_sec_chunks').between(start, end, index='start_time').run(conn)

		data = list(cursor)

		for chunk in data:
			chunk['start_time'] = chunk['start_time'].strftime("%Y-%m-%d %H:%M:%S")

		channels = data[0]['streams'].keys()

		data1 = {}
		data2 = {}

		for channel in channels:
			data1[channel] = {}
			data2[channel] = {}
			data1[channel]['joinedCorpus'] = ''
			data2[channel]['totalMsgCount'] = 0

# consolidate text corpus and message frequency

		for x in data:
			for y in channels:
				if y in x['streams']:
					for z in x['streams'][y]['corpus']:
						data1[y]['joinedCorpus'] += z
					data2[y]['totalMsgCount'] += x['streams'][y]['message_frequency']


		for streamName, streamObject in data1.iteritems():
			corpusArray = [word.lower() for word in streamObject['joinedCorpus'].encode('utf-8', 'replace').split()]
			data2[streamName]['freqData'] = FreqDist(corpusArray).pformat(3).strip('FreqDist(').strip(', ...})')
			data2[streamName]['freqData'] += '}'
#			data2[streamName]['bigramFreq'] = FreqDist(bigrams(corpusArray)).pformat(3).strip('FreqDist(').strip(', ...})')
#			data2[streamName]['bigramFreq'] += '}'
#			data2[streamName]['trigramFreq'] = FreqDist(trigrams(corpusArray)).pformat(3).strip('FreqDist(').strip(', ...})')
#			data2[streamName]['trigramFreq'] += '}'


		return json.dumps(data2)




class FetchVodData(MethodView):

	def get(self):

		required_parameters = ('start', 'end')

		if None in map(request.args.get, required_parameters):
			return "Error: URI missing parameter - start and end indexes required"
		
		start_index = int(request.args.get('start'))
		end_index = int(request.args.get('end'))

		data = generateJSONData(start_index, end_index)

		return data

class HighlightData(MethodView):

	def post(self):

		import re, pdb

		request.get_json()

		ip_address = request.environ['REMOTE_ADDR']
		data = json.loads(request.data)

		title = data['title']
		twitch_id = data['twitch_id']
		current_time = datetime.utcnow()

		url_friendly_title = title.encode('ascii', 'ignore')
		url_friendly_title = url_friendly_title.replace(' ', '-')
		url_friendly_title = re.sub('[^A-Za-z0-9-]+', '', url_friendly_title)

		same_titles = VodHighlightData.objects(url_friendly_title=url_friendly_title)
		duplicate = same_titles.filter(Q(origin_ip=ip_address) & Q(vod_id=twitch_id))


		if len(same_titles) > 0:
			
			if len(duplicate) > 0:
			
				duplicate[0].update(data=request.data)
				output = {'status': 'success', 'url': duplicate[0].url}

				return json.dumps(output)

			else:

				url = url_friendly_title + '-' + str(len(same_titles))

		else:

			url = url_friendly_title
		
		new_data_object = VodHighlightData(origin_ip=ip_address, data=request.data, title=title, url_friendly_title=url_friendly_title, url=url, vod_id=twitch_id, creation_date=current_time)
		new_data_object.save()

		output = {'status': 'success', 'url': url}

		return json.dumps(output)


class HighlightReel(MethodView):

	@cache.memoize(timeout=60)
	def get(self, title_url):

		highlight_data = VodHighlightData.objects(url=title_url)

		if len(highlight_data) < 1:
			return 'Error: Bad URL'

		vod_id = highlight_data[0].vod_id
		highlight_string = highlight_data[0].data

		data = generateJSONData(vod_id, [])

		return render_template('frontpage.html', data=data, highlights=highlight_string)


class FrontPage(MethodView):

	@cache.cached(timeout=60)
	def get(self):

		data = generateJSONData(0,5)

		return render_template('frontpage.html', data=data)


class NewFrontPage(MethodView):

	@cache.cached(timeout=60)
	def get(self):

		data = generateJSONData(0,5)

		return render_template('newfrontpage.html', data=data)


class SingleView(MethodView):

	def get(self, vod_id):

		data = generateJSONData(vod_id, [])

		return render_template('frontpage.html', data=data)



class HistogramView(MethodView):	

	def get(self, vod_id):

		from numpy import histogram, mean, std, delete
		from math import exp, sqrt
		from elasticsearch import Elasticsearch
		import re

		if request.args.get('descriptor') == None:
			return "Error: URI missing descriptor"

		descriptor = request.args.get('descriptor')

		if request.args.get('query') == None:
			query = Descriptor.objects(descriptor=descriptor)[0].default_query
		else:
			query = request.args.get('query').lower()

		# format query for elasticsearch
		query.replace('+', ' AND ').replace('|', ' OR ')

		data = VodHistogramData.objects(Q(vod_id=vod_id) & Q(descriptor=descriptor))

		# don't query elasticsearch if descriptor and query have already been chached
		if (len(data) != 0):
			if (query == data[0].query):
				return json.dumps(data[0].data)

		# otherwise delete the data for the descriptor and 
		data.delete()

		# get vod data for elasticsearch query
		vod = Vod.objects(twitch_id=vod_id)

		start_time = vod[0].start_time
		duration = vod[0].duration

		date_string = str(start_time.year) + '-' + str(start_time.month).zfill(2) + '-' + str(start_time.day).zfill(2)
		time_string = str(start_time.hour).zfill(2) + ':' + str(start_time.minute).zfill(2) + ':' + str(start_time.second).zfill(2)
		start_string = date_string + ' ' + time_string


		# get messages matching query string from the elasticsearch server
		messages, seconds_elapsed = stringQueryElasticsearch(vod_id, query, start_string, duration)

		# some keywords have a stronger correlation with certain moments, so we add duplicate occurences to the histogram
		stronger_words = re.compile('god|lmfao')

		for i, message_content in enumerate(messages):
			if stronger_words.search(message_content.lower()):
				seconds_elapsed.append(seconds_elapsed[i])
				seconds_elapsed.append(seconds_elapsed[i])

		# produce histogram based on the occurence of targeted words
		histogram_data, bins = histogram(seconds_elapsed, range(0,max(seconds_elapsed),15))



		# fetch viewer counts to adjust message occurrence rates as a function of the number of viewers present
		viewer_count_array_30s, viewer_average, viewer_array_seconds_offset = getViewerArray(vod_id, start_string, duration)
		# viewer data collected every 30s, so we have to create a new array with 15s intervals by duplicating the datapoints
		viewer_count_array_15s = []
		for viewer_count in viewer_count_array_30s:
			viewer_count_array_15s.append(viewer_count)
			viewer_count_array_15s.append(viewer_count)

		# adjust array size to account for delay in viewer stat collection
		for i in range(0, int(viewer_array_seconds_offset/15)):
			viewer_count_array_15s.insert(0,0)

		# adjust array size to be minimum as long as histogram_data array
		while len(viewer_count_array_15s) < len(histogram_data):
			viewer_count_array_15s.append(0)

		# construct viewer adjusted histogram data x*sqrt(avgviewers/viewers) - with an adjustment factor no larger than 3
		viewer_adjusted_histogram_data = [occurrences*min(3, sqrt((viewer_average)/float(1+viewer_count_array_15s[i]))) for i, occurrences in enumerate(histogram_data)]

		# grab stat values for normalization function

		maximum_bin_value = max(viewer_adjusted_histogram_data)
		average_bin_value = mean(viewer_adjusted_histogram_data)
		standard_deviation = std(viewer_adjusted_histogram_data)
		k = 0.75/standard_deviation

		# apply a logistic function to the data and normalize to int values between 0-100
		normalized_histogram_data = map(lambda value: 0 if value < 10 else value, map(lambda x: int(100/(1+exp(-k*(x-(1+average_bin_value+3*standard_deviation))))), viewer_adjusted_histogram_data))

		db_object = VodHistogramData(vod_id=vod_id, query=query, descriptor=descriptor, data=normalized_histogram_data)
		db_object.save()

		return json.dumps(normalized_histogram_data) #+ 'k: ' + str(k) + '   std: ' + str(standard_deviation) + '    avg:' + str(average_bin_value) + '    nonzero histo:' + ', '.join(str(e) for e in non_zero_histogram_data)





#### In development funcs #######


# API endpoint for getting specified message data
class HistogramSliceView(MethodView):

	def get(self):

		required_parameters = ('channel', 'start', 'duration', 'query')

		if None in map(request.args.get, required_parameters):
			return "Error: URI missing parameter - channel, start, duration and/or query"
		if (len(request.args.get('start')) != 14):
			return "Error: Start time parameter improper length - please include a number of length 14 with format YYYYmmddHHMMSS"

		channel = request.args.get('channel')
		start_time = request.args.get('start')
		duration = request.args.get('duration')
		query_string = request.args.get('query')

		# TODO check database for already saved data



		# format query for elasticsearch
		query_string.replace('+', ' AND ').replace('|', ' OR ')

		date_string = start_time[0:4] + '-' + start_time[4:6] + '-' + start_time[6:8]
		time_string = start_time[8:10] + ':' + start_time[10:12] + ':' + start_time[12:14]
		start_string = date_string + ' ' + time_string

		index_string = channel + '__' + date_string

		ip_string = "http://45.55.216.78:9200"
		url_string = ip_string + '/' + index_string + "/_search?size=99999"

		# a lot of the magic happens here - check data_methods.py for reference
		messages, seconds_elapsed = stringQueryElasticsearch(url_string, query_string, start_string, duration)

		output = []

		for i, msg in enumerate(messages):
			output.append({"content" : msg, "time" : seconds_elapsed[i]})


		return json.dumps({"messages" : output})


class MinuteOfMessages(MethodView):

	def get(self):

		params = ('vod_id', 'start_time')

		if None in map(request.args.get, params):
			return "Error: URI missing parameter - vod_id and/or start_time"

		vod_id = request.args.get('vod_id')
		start_time = request.args.get('start_time')

		messages = ChatMessage.objects(Q(vod_id=vod_id) & Q(seconds_elapsed__gte=start_time) & Q(seconds_elapsed__lt=start_time+60))





# Register the urls
twitch_vods.add_url_rule('/', view_func=FrontPage.as_view('front'))
twitch_vods.add_url_rule('/newfront/', view_func=NewFrontPage.as_view('newfront'))
twitch_vods.add_url_rule('/highlight-data/', view_func=HighlightData.as_view('savehighlights'))
twitch_vods.add_url_rule('/highlights/<title_url>', view_func=HighlightReel.as_view('watchhighlights'))
twitch_vods.add_url_rule('/getvods/', view_func=FetchVodData.as_view('getvods'))
twitch_vods.add_url_rule('/vod/<vod_id>', view_func=SingleView.as_view('vod'))
twitch_vods.add_url_rule('/messages/', view_func=MinuteOfMessages.as_view('messages'))
twitch_vods.add_url_rule('/histogram/<vod_id>', view_func=HistogramView.as_view('histogram'))
twitch_vods.add_url_rule('/histogramslice/', view_func=HistogramSliceView.as_view('histogram slice'))
twitch_vods.add_url_rule('/chatstream/', view_func=ChatStream.as_view('chatstream'))
twitch_vods.add_url_rule('/chatstreamstats/', view_func=ChatStreamStats.as_view('chatstreamstats'))
twitch_vods.add_url_rule('/chatstreamstats10/', view_func=ChatStreamStats10.as_view('chatstreamstats10'))