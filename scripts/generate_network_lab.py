import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--state-root',type=Path,required=True);p.add_argument('--image',default='p2p-network-lab:local');p.add_argument('--restricted',action='store_true');p.add_argument('--strategy',choices=['random','rating'],default='random');a=p.parse_args()
a.root.mkdir(parents=True,exist_ok=True); (a.root/'results').mkdir(exist_ok=True)
ids=(a.root/'ids.txt').read_text(encoding='utf-8-sig').splitlines()
networks={'left':{'internal':True,'ipam':{'config':[{'subnet':'172.29.211.0/24'}]}}}
if a.restricted: networks['right']={'internal':True,'ipam':{'config':[{'subnet':'172.29.212.0/24'}]}}
services={}
for i in range(7):
 side='right' if a.restricted and i>4 else 'left'; prefix='172.29.212' if side=='right' else '172.29.211'; ip=f'{prefix}.{10+i}'
 nets={side:{'ipv4_address':ip}}
 if a.restricted and i==4: nets['right']={'ipv4_address':'172.29.212.14'}
 cfg={'node':{'port':9000+i,'advertise_address':f'{ip}:{9000+i}','state_dir':'/app/state','read_timeout':'5s','write_timeout':'5s'},'protocol':{'version':1,'codec':'msgpack'},'dht':{'k':4,'alpha':3,'replication':3,'ttl_seconds':180,'refresh_interval':'3s','republish_interval':'60s','relay_strategy':a.strategy},'tunnel':{'relays':3,'pool_size':3},'bootstrap':{'peers':[] if i==4 else ['node-4:9004'],'pins':{} if i==4 else {'node-4:9004':ids[4]}},'experiment':{'enabled':i==0,'delay':'25s','repeats':6,'interval':'1s','targets':[ids[3]],'output_dir':'/app/results'}}
 config=a.root/f'node-{i}.json';config.write_text(json.dumps(cfg,indent=2))
 services[f'node-{i}']={'image':a.image,'networks':nets,'cap_add':['NET_ADMIN'],'volumes':[f'{(a.state_root/f"node-{i}").resolve().as_posix()}:/app/state',f'{config.resolve().as_posix()}:/app/config.json:ro',f'{(a.root/"results").resolve().as_posix()}:/app/results'],'command':['/app/config.json']}
(a.root/'compose.json').write_text(json.dumps({'services':services,'networks':networks},indent=2))
