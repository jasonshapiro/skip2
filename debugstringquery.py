from dateutil import parser
import requests, json, datetime
from env_variables import *

import pdb

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

	pdb.set_trace()

	# two search requests, one to return first 10 hits and total hits, and 2nd to return all hits (elasticsearch makes you do this)
	init_response = es.search(index=vod_id, doc_type='message', body=query)
	size = init_response['hits']['total']

	data = es.search(index=vod_id, doc_type='message', body=query, size=size)

	start_datetime = parser.parse(start_string).replace(tzinfo=None)

	messages = [msg['_source']['content'] for msg in data['hits']['hits']]
	time = [datetime.datetime.strptime(msg['_source']['time'], '%Y-%m-%d %H:%M:%S') for msg in data['hits']['hits']]
	seconds_elapsed = [(t - start_datetime).seconds for t in time]

	pdb.set_trace()

	return messages, seconds_elapsed


print(stringQueryElasticsearch('v91246843','lol+haha+hahaha+hahahaha+rofl+lmao+lmfao+elegiggle+4head+lolol', '2016-09-25 02:34:47', 18414))