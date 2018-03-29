import datetime
from flask import url_for
from skiptwo import db


class Vod(db.Document):
    start_time = db.DateTimeField(default=datetime.datetime.now, required=True)
    duration = db.IntField(required=True)
    channel = db.StringField(max_length=255, required=True)
    title = db.StringField(required=True)
    url = db.StringField(required=True)
    twitch_id = db.StringField(required=True)
    avg_viewer_count = db.IntField()
    game = db.StringField(max_length=255)

    meta = {
        'allow_inheritance': True,
        'ordering': ['+start_time']
    }

class ProtoVod(db.Document):
    start_time = db.DateTimeField(default=datetime.datetime.now, required=True)
    channel = db.StringField(max_length=255, required=True)


class Channel(db.Document):
    name = db.StringField(required=True)
    image = db.StringField(required=True)
    score = db.FloatField(default=1, required=True)
    language = db.StringField(max_length=255)


class VodHighlightData(db.Document):
    origin_ip = db.StringField(required=True)
    data = db.StringField(required=True, max_length=99999)
    title = db.StringField(required=True)
    url_friendly_title = db.StringField(required=True)
    url = db.StringField(required=True)
    vod_id = db.StringField(required=True)
    creation_date = db.DateTimeField()


class VodStats(db.Document):
    vod_id = db.StringField(required=True)
    seconds_elapsed = db.IntField(required=True)
    msg_per_min_45s = db.FloatField()
    msg_per_min_5m = db.FloatField()
    viewer_count = db.IntField()

    meta = {
        'indexes': ['seconds_elapsed'],
        'ordering' : ['+seconds_elapsed']
    }


class Stat(db.Document):
    stat_type = db.StringField(required=True)
    stat_value = db.FloatField()
    stat_time = db.DateTimeField()
    vod_id = db.StringField(required=True)


class Descriptor(db.Document):
    descriptor = db.StringField(required=True)
    default_query = db.StringField(required=True)

class VodHistogramData(db.Document):
    vod_id = db.StringField(required=True)
    query = db.StringField(required=True)
    descriptor = db.StringField(required=True)
    data = db.ListField(db.IntField(), required=True)


class ChatMessage(db.Document):
    vod_id = db.StringField(required=True)
    seconds_elapsed = db.IntField(required=True)
    username = db.StringField(required=True)
    content = db.StringField(required=True)

    meta = {
        'indexes': ['seconds_elapsed'],
        'ordering': ['+seconds_elapsed']
    }

