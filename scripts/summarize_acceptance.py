"""Verify raw acceptance scenarios and produce statistics without small-sample percentiles."""
import argparse,csv,json,statistics
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--dir',required=True);p.add_argument('--repeats',type=int,default=5);a=p.parse_args();root=Path(a.dir);groups={}
def target(r):
 v=r['target'];return bytes(v).hex() if isinstance(v,list) else v
for path in sorted(root.glob('n*-*/scenario.json')):
 d=json.loads(path.read_text());group=(d['N'],d['K'],d['Scheme']);groups.setdefault(group,[]).append(d)
 if 'Replication' in d:
  r=d['Replication'];holders=r['holders'];ids=[h['node_id'] for h in holders]
  assert len(ids)==3 and len(set(ids))==3,f'{path}: three distinct remote holders required'
  assert r['local_copy']['node_id'] not in ids and r['owner'] not in ids,f'{path}: local/owner copy counted as a remote holder'
  if 'probes' in r:
   assert [p['node_id'] for p in r['probes']]==r['candidate_ids'][:len(r['probes'])],f'{path}: candidate order bypassed'
   assert [p['node_id'] for p in r['probes'] if p['available']]==ids,f'{path}: wrong nearest available selection'
  assert r['confirmations']==sum(h['acknowledged'] for h in holders) and r['confirmations']>=2,f'{path}: remote quorum not confirmed'
 for flag in ['CandidateFailure','HolderValue','SeedLookup','SeedValue','RelayRecovery']:
  assert d.get(flag),f'{path}: {flag} failed'
 assert d['FullTables']*5<=d['N'],f'{path}: full tables'
 assert len(d['Lookups'])>=30 and len(d['WarmMessageMS'])>=30,f'{path}: insufficient observations'
 assert len(list(path.parent.glob('metrics-*.json')))==d['N'],f'{path}: missing metrics'
 for r in d['Lookups']:
  t=target(r);assert t not in (r['initial_closest'] or []),f'{path}: target evidence'
  assert any(q!=t for it in r['trace'] for q in it['queried']),f'{path}: no intermediate'
  assert r['rpcs']==sum(len(it['queried']) for it in r['trace']),f'{path}: RPC mismatch'
 assert sum(target(q) in q['closest'] for q in d['Lookups'])/len(d['Lookups'])>=.95,f'{path}: lookup success rate below 95%'
 assert d['CandidateTrace']['errors']>0,f'{path}: no candidate failure observed'
 assert len(d['Holders'])>=3,f'{path}: missing holders'
 if len(d['Rounds'])<3:raise AssertionError(f'{path}: insufficient convergence rounds')
 for prev,cur in zip(d['Rounds'][-3:-1],d['Rounds'][-2:]):
  assert cur['Success']/cur['Total']>=.95 and abs(cur['Contacts']-prev['Contacts'])/prev['Contacts']<=.1,f'{path}: convergence failed'
if not groups:raise SystemExit('No acceptance scenarios')
rows=[]
for (n,k,scheme),runs in sorted(groups.items()):
 assert len(runs)==a.repeats,f'{n}/{scheme}: expected {a.repeats} repetitions, got {len(runs)}'
 metrics={'lookup_success_rate':[sum(target(q) in q['closest'] for q in r['Lookups'])/len(r['Lookups']) for r in runs],'convergence_ms':[r['ConvergenceMS'] for r in runs],'recovery_ms':[r['RecoveryMS'] for r in runs],'first_message_ms':[r['FirstMessageMS'] for r in runs],'warm_message_ms':[x for r in runs for x in r['WarmMessageMS']],'lookup_ms':[q['duration_ms'] for r in runs for q in r['Lookups']],'lookup_rpcs':[q['rpcs'] for r in runs for q in r['Lookups']],'lookup_iterations':[q['iterations'] for r in runs for q in r['Lookups']]}
 for name,values in metrics.items():
  ordered=sorted(values);row={'N':n,'k':k,'scheme':scheme,'metric':name,'observations':len(values),'min':min(values),'max':max(values),'median':statistics.median(values),'mean':statistics.mean(values),'p95':ordered[max(0,(95*len(ordered)+99)//100-1)] if len(values)>=20 else ''};rows.append(row)
with (root/'summary.csv').open('w',newline='',encoding='utf-8') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
print(f'PASS: {len(groups)} configurations, {sum(map(len,groups.values()))} independent runs; summary.csv saved')
