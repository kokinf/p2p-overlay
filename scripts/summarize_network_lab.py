import argparse,re,csv,statistics,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--dir',type=Path,required=True);a=p.parse_args();rows=[];summary=[]
for mode in ['baseline-random','baseline-rating','degraded-random','degraded-rating','restricted-random']:
 log=(a.dir/mode/'node-0.log').read_text(encoding='utf-8-sig')
 all_log=(a.dir/mode/'all.log').read_text(encoding='utf-8-sig')
 receiver=(a.dir/mode/'ids.txt').read_text(encoding='utf-8-sig').splitlines()[3]
 deliveries=len(re.findall(r'\[APP\] '+receiver+r':',all_log))
 hops={}
 for relay,tid,seq in re.findall(r'\[TUNNEL_HOP\] node=([0-9a-f]+) tunnel=([0-9a-f]+) seq=(\d+)',all_log):hops.setdefault((tid,seq),set()).add(relay)
 if deliveries!=6 or len(hops)!=6 or any(len(v)!=3 for v in hops.values()):raise SystemExit(f'Missing delivery or three-hop forwarding evidence: {mode}')
 found=re.findall(r'\[EXPERIMENT\] sample=(\d+) operation=(\w+) success=(true|false) duration_ms=(\d+)',log)
 for sample,op,success,ms in found:rows.append(dict(mode=mode,sample=int(sample),operation=op,success=success=='true',duration_ms=int(ms),phase='cold' if sample=='0' else 'warm'))
 ids=(a.dir/mode/'ids.txt').read_text(encoding='utf-8-sig').splitlines()
 metrics=json.loads(next((a.dir/mode/'results').glob('metrics-*.json')).read_text(encoding='utf-8-sig'))
 target=list(bytes.fromhex(ids[3]))
 controls=[r for r in metrics['lookups'] if r['kind']=='node' and r['target']==target]
 if len(controls)!=6:raise SystemExit(f'Expected six explicit target traces in {mode}, got {len(controls)}')
 lookup_rows=[r for r in rows if r['mode']==mode and r['operation']=='lookup']
 for row,trace in zip(lookup_rows,controls):row['success']=row['success'] and ids[3] in (trace.get('closest') or [])
 for op in ['lookup','message']:
  group=[r for r in rows if r['mode']==mode and r['operation']==op]
  if len(group)!=6 or len({r['sample'] for r in group})!=6:raise SystemExit(f'Missing samples: {mode}/{op}')
  values=[r['duration_ms'] for r in group if r['success'] and r['phase']=='warm']
  summary.append(dict(mode=mode,operation=op,n=len(group),success=sum(r['success'] for r in group),warm_n=len(values),warm_mean_ms=statistics.mean(values) if values else '',warm_median_ms=statistics.median(values) if values else '',warm_min_ms=min(values) if values else '',warm_max_ms=max(values) if values else ''))
for name,data in [('samples.csv',rows),('summary.csv',summary)]:
 with (a.dir/name).open('w',newline='',encoding='utf-8') as f:
  w=csv.DictWriter(f,fieldnames=list(data[0]));w.writeheader();w.writerows(data)
print(f'Saved {len(rows)} measurements. Failed operations retained; inspect summary.csv.')

restricted=a.dir/'restricted-random'
for name in ['reachable.txt','bridge-reachable.txt']:
 if 'TCP reachable' not in (restricted/name).read_text(encoding='utf-8-sig'):raise SystemExit(f'Missing positive TCP control: {name}')
blocked=(restricted/'blocked.txt').read_text(encoding='utf-8-sig')
if 'dial tcp' not in blocked:raise SystemExit('Missing failed TCP connection evidence')
for mode in ['degraded-random','degraded-rating']:
 for i in [1,2]:
  text=(a.dir/mode/f'netem-{i}.txt').read_text(encoding='utf-8-sig')
  if 'delay 80ms' not in text or 'loss 2%' not in text:raise SystemExit('Missing netem state')
report=dict(attempts=len(rows),successful=sum(r['success'] for r in rows),restricted_positive_pairs=2,restricted_blocked_pairs=1,netem_configurations=2)
(a.dir/'verification.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report))
