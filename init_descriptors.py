import manage
from skiptwo.models import *

descriptor_list = ['plays', 'funny', 'throws', 'weird', 'babyrage', 'salt']
query_list = ['pogchamp+kreygasm+god+woaw+wow+player+god', 'lol+haha+hahaha+hahahaha+rofl+lmao+lmfao+elegiggle+4head+lolol', '322+rekt+throw+throws', 'wutface, dansgame', 'babyrage', 'pjsalt+salt+salty']

for i, descriptor in enumerate(descriptor_list):
	new_descriptor = Descriptor(descriptor=descriptor, default_query=query_list[i])
	new_descriptor.save()
