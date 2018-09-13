import os.path
import glob
import json


def from_loghub(path):
    files = glob.glob(os.path.join(path, "sched*"))
    jsons = []
    for p in files:
        with open(p) as f:
            jsons.append(json.load(f))

    return trans(jsons)


def fmt_duraion(s):
    i = int(s)
    r = ""
    for a, b in [(86400, 'd'), (3600, 'h'), (60, 'm')]:
        if i >= a:
            r += ("{}" + b).format(i / a)
            i %= a
    if r == "":
        # return "{:.1e}s".format(s)
        return "{}s".format(i + 1)
    else:
        return r


def M(b):
    return int(b/(1024*1024))


def summary_prof(p):
    counters = p['counters']
    stats = p['stats']
    info = p['info']

    task = counters['task']
    fail = counters['fail']
    mem = stats['bytes_max_rss']
    t = stats['secs_all']
    task_torun = task['all'] - task['running'] - task['finished']
    fail_error = fail['all'] - fail['oom'] - fail['timeout']
    finished = task['finished']
    if info['finish_time'] > 0:
        time_stage = info['finish_time'] - info['start_time']
    else:
        time_stage = "None"
    if finished > 0:
        avg_mem = mem['sum'] / finished
        avg_time = t['sum'] / finished
        speedup = avg_time * finished / time_stage
    else:
        avg_mem = 0
        avg_time = 0
        speedup = 0

    return [
        ["task", "{} = {} + {} + {}".format(task['all'], task['finished'], task['running'], task_torun)],
        ["fail", "{} = {} + {} + {}".format(fail['all'], fail['oom'],  fail_error, fail['timeout'])],
        ['mem',  "{} || [{}, {}, {}]".format(info['mem'], M(mem['min']), M(avg_mem), M(mem['max']))],
        ['time', "{} || [{}, {}, {}]".format(*[fmt_duraion(s) for s in [time_stage, t['min'], avg_time, t['max']]])],
        ['speedup',  "{:.2f}".format(speedup)]
    ]


def trans(runs):
    api_nodes = []
    api_edges = []
    stage_nodes = []
    stage_edges = []
    for r in runs:
        r = r["run"]
        for s in r['stages']:
            for n in s['graph']['nodes']:
                rdds = n['rdds']
                n['rdds'] = list(reversed([{"k": rdd["rdd_name"], "v": str(rdd["scope_id"])}
                                           for rdd in rdds]))
                if n['id'] == s['info']['output_pipeline']:
                    p = n['prof'] = {
                        'info': s['info'],
                        'stats': s['stats'],
                        'counters': s['counters']
                    }
                    n['prof_summary'] = summary_prof(p)
                    n['is_output'] = True
                else:
                    n['is_output'] = False

                stage_nodes.append(n)
            for e in s['graph']['edges']:
                e['IO'] = "1M"
                stage_edges.append(e)
        sink_node = r["sink"]['node']
        sink_node['call_id'] = str(sink_node['call_id'])
        stage_nodes.append(sink_node)
        stage_edges.append(r['sink']['edges'])

        c = r['call_graph']
        for n in c['nodes']:
            n['id'] = n['call_id'] = str(n['id'])
            api_nodes.append(n)
        for e in c['edges']:
            e['source'] = str(e['source'])
            e['target'] = str(e['target'])
            api_edges.append(e)
    res = {
        "stages": {
            "nodes": stage_nodes,
            "edges": stage_edges,
        },
        "calls": {
              "nodes": api_nodes,
              "edges": api_edges,
        }
    }

    return res
