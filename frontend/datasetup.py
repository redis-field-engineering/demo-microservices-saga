from redisearch import Client, IndexDefinition, TextField, NumericField, TagField

def setup_data(cfg):
   load_client = Client(
      'MicroServiceSaga-v1',
      host=cfg['host'],
      #password=cfg['password'],
      port=cfg['port']
   )
   
   definition = IndexDefinition(
           prefix=['STATE:'],
           language='English',
           score_field='id',
           score=0.5
           )
   flds = [TextField("id", weight=5.0), TagField('tags')]
   for j in cfg['microservices'][1:]:
           flds.append(TextField(j['name'], sortable=True))
           flds.append(NumericField('%s_RETRY' %j['name'], sortable=True))
   load_client.create_index(
           (
                   tuple(flds)
               ),        
       definition=definition)

   # Finally Create the alias
   load_client.aliasadd("MicroServiceSaga")
