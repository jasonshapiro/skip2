from dateutil import parser
import requests, json, datetime
from env_variables import *


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
def stringQueryElasticsearch(vod_id, query_string, start_string, duration):

	from elasticsearch import Elasticsearch

	query = {
		    "query": {
   				"query_string" : {
   				 	"default_field" : "content",
   				 	"query" : query_string }
   				 	}
			}

	es = Elasticsearch(ELASTICSEARCH_ENDPOINT)

	# two search requests, one to return first 10 hits and total hits, and 2nd to return all hits (elasticsearch makes you do this)
	init_response = es.search(index=vod_id, doc_type='message', body=query)
	size = init_response['hits']['total']

	data = es.search(index=vod_id, doc_type='message', body=query, size=size)

	start_datetime = parser.parse(start_string).replace(tzinfo=None)

	messages = [msg['_source']['content'] for msg in data['hits']['hits']]
	time = [datetime.datetime.strptime(msg['_source']['time'], '%Y-%m-%d %H:%M:%S') for msg in data['hits']['hits']]
	seconds_elapsed = [(t - start_datetime).seconds for t in time]

	return messages, seconds_elapsed


def getAverageViewerCount(vod_id, start_string, duration):

	from elasticsearch import Elasticsearch

	es = Elasticsearch(ELASTICSEARCH_ENDPOINT)

	query = {
				"filter" : {
					"range" : {"time": { "gte" : start_string ,
										 "lte" : start_string + "||+" + str(duration) + "s" }
					}
				}
	}

	try:
		viewer_data = es.search(index=vod_id, doc_type='stats', body=query, size=((duration/30)+1))
		viewer_counts = [int(data_sample['_source']['viewers']) for data_sample in viewer_data['hits']['hits']]
		return sum(viewer_counts)/len(viewer_counts)
	except:
		return 1000

def getViewerArray(vod_id, start_string, duration):

	from elasticsearch import Elasticsearch

	es = Elasticsearch(ELASTICSEARCH_ENDPOINT)

	query = {
				"filter" : {
					"range" : {"time": { "gte" : start_string ,
										 "lte" : start_string + "||+" + str(duration) + "s" }
					}
				},
				"sort": { "time": { "order": "asc" }}
	}

	viewer_data = es.search(index=vod_id, doc_type='stats', body=query, size=((duration/30)+1))
	viewer_counts = [int(data_sample['_source']['viewers']) for data_sample in viewer_data['hits']['hits']]
	viewer_average = sum(viewer_counts)/len(viewer_counts)

	# get first recorded stat and return offset (in seconds) for when to start array
	offset = (string_to_datetime(viewer_data['hits']['hits'][0]['_source']['time']) - string_to_datetime(start_string)).total_seconds()

	return viewer_counts, viewer_average, offset