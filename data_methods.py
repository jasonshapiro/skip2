from dateutil import parser
import requests, json, datetime

def datetime_to_string(obj, format):
	
	if format == 'url_string':
		string = obj.strftime('%Y%m%d%H%M%S')
	else:
		string = obj.strftime('%Y-%m-%d %H:%M:%S')

	return string

def string_to_datetime(string):

	obj = parser.parse(string).replace(tzinfo=None)

	return obj


def in_daterange(start_datestring, duration, checked_datestring):

	start = string_to_datetime(start_datestring)
	end = start + datetime.timedelta(seconds=duration)
	checked = string_to_datetime(checked_datestring)

	if (start <= checked) & (end >= checked):
		return True
	else:
		return False

# Pull an array of {message, seconds_since_starttime} objects from a url-specified elasticsearch index matching query-specified text criteria
# query_string format : "(word_to_search_for)+(other_word_to_search_for)+...."
def stringQueryElasticsearch(url_string, query_string, start_string, duration):

	def _request_to_message_array(url_string, query_string, start_string, duration):

		query = {
				"query": {
					"filtered": {
						"query": {
			   				"query_string" : {
			   				 	"default_field" : "content",
			   				 	"query" : query_string }},

			            "filter": { 
			            	"range" : {
			            		"time": { "gte": start_string ,
			            				"lte" : start_string + '||+' + str(duration) + 's' }}},
			        }
			    },
			    "sort": { "time": { "order": "asc" }}
			}

		r = requests.post(url_string, data=json.dumps(query))
		data = r.json()

		start_datetime = parser.parse(start_string).replace(tzinfo=None)
		end_datetime = start_datetime + datetime.timedelta(seconds=int(duration))

		messages = [msg['_source']['content'] for msg in data['hits']['hits']]
		time = [datetime.datetime.strptime(msg['_source']['time'], '%Y-%m-%d %H:%M:%S') for msg in data['hits']['hits']]
		seconds_elapsed = [(t - start_datetime).seconds for t in time]

		return messages, seconds_elapsed

	messages, seconds_elapsed = _request_to_message_array(url_string, query_string, start_string, duration)

	start_datetime = parser.parse(start_string).replace(tzinfo=None)
	end_datetime = start_datetime + datetime.timedelta(seconds=int(duration))


	# for VoDs that are recorded over a 2 day period (e.g. 8/4 @ 22:00 to 8/5 @ 5:00), we must query two separate elasticsearch indices due to how the data is stored
	if (start_datetime.day != end_datetime.day):
		day_two_date_string = str(end_datetime.year) + '-' + str(end_datetime.month).zfill(2) + '-' + str(end_datetime.day).zfill(2)
		day_two_url_string = url_string.split('__')[0] + '__' + day_two_date_string + "/_search?size=99999"
		day_two_start_string = day_two_date_string + " 00:00:00"
		day_two_duration = (end_datetime - datetime.datetime(end_datetime.year, end_datetime.month, end_datetime.day, 0, 0)).seconds

		day_two_msg, day_two_sec_unadjusted = _request_to_message_array(day_two_url_string, query_string, day_two_start_string, day_two_duration)

		day_two_midnight_datetime = datetime.datetime(end_datetime.year, end_datetime.month, end_datetime.day, 0, 0, 0)
		seconds_until_day_two = (day_two_midnight_datetime - start_datetime).seconds

		day_two_sec = map(lambda x: x + seconds_until_day_two, day_two_sec_unadjusted)

		messages = messages + day_two_msg
		seconds_elapsed = seconds_elapsed + day_two_sec

	return messages, seconds_elapsed

def getAverageViewerCount(channel, start_time):

	from elasticsearch import Elasticsearch

	es = Elasticsearch(['http://45.55.216.78:9200/'])
	index = channel + '__' + str(start_time.year).zfill(2) + '-' + str(start_time.month).zfill(2) + '-' + str(start_time.day).zfill(2)
	try:
		viewer_data = es.search(index=index, doc_type='stats', size=99999)
		viewer_counts = [int(data_sample['_source']['viewers']) for data_sample in viewer_data['hits']['hits']]
		return sum(viewer_counts)/len(viewer_counts)
	except:
		return 0