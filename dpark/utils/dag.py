"""
TODO:

- show size/time info for input/output/shuffle.
- show broadcast cache, checkpoint.
- show src file content on click (in a new tab).
- Try to get failed job/stage dag.
- Make struct of .prof more stable, decouple it from view.

"""

import os.path
import glob
import json
import time


KW_NODES = "nodes"
KW_ID = "id"  # uniq, use in edges
KW_LABEL = "label"  # short name
KW_DETAIL = "detail"  # show when hover
KW_TYPE = "type"

KW_EDGES = "edges"
KW_SRC = "source"
KW_DST = "target"


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

    task_torun = task['all'] - task['running'] - task['finished']
    other_error = fail['all'] - fail['oom'] - fail['run_timeout'] - fail['staging_timeout'] - fail['fetch']
    finished = task['finished']
    res = [
        ["task", "{} = {} + {} + {}".format(task['all'], task['finished'], task['running'], task_torun)],
        ["fail", "{} = {} + {} + {} + {} + {} ".format(fail['all'],
                                                       fail['oom'],
                                                       fail['fetch'],
                                                       fail['run_timeout'],
                                                       fail['staging_timeout'],
                                                       other_error
                                                       )],
    ]

    if not stats:
        return res

    mem = stats['bytes_max_rss']
    t = stats['secs_all']

    end_time = info['finish_time']
    if end_time <= 0:
        end_time = time.time()

    stage_time = end_time - info['start_time']
    if finished > 0:
        avg_mem = mem['sum'] / finished
        avg_time = t['sum'] / finished
        speedup = avg_time * finished / stage_time
    else:
        avg_mem = 0
        avg_time = 0
        speedup = 0

    res2 = [
        ['mem',  "{} || [{}, {}, {}]".format(info['mem'], M(mem['min']), M(avg_mem), M(mem['max']))],
        ['time', "{} || [{}, {}, {}]".format(*[fmt_duraion(s) for s in [stage_time, t['min'], avg_time, t['max']]])],
        ['speedup',  "{:.2f}".format(speedup)]
    ]
    return res + res2


def trans(runs):
    api_nodes = {}
    api_edges = {}
    stage_nodes = {}
    stage_edges = {}
    for r in runs:
        r = r["run"]
        for s in r['stages']:
            for n in s['graph'][KW_NODES]:
                if n[KW_ID] in stage_nodes:
                    continue
                rdds = n['rdds']
                n['rdds'] = list(reversed([{"k": rdd["rdd_name"],
                                            "v": str(rdd["api_callsite_id"]),
                                            "params": rdd['params']}
                                           for rdd in rdds]))
                if n[KW_ID] == s['info']['output_pipeline']:
                    p = n['prof'] = {
                        'info': s['info'],
                        'stats': s['stats'],
                        'counters': s['counters']
                    }
                    n['prof_summary'] = summary_prof(p)
                    n['is_output'] = True
                else:
                    n['is_output'] = False

                stage_nodes[n[KW_ID]] = n
            for e in s['graph'][KW_EDGES]:
                id_ = e[KW_SRC], e[KW_DST]
                if id_ not in stage_edges:
                    e['info'] = ",".join(["{}={}".format(str(k), str(v)) for k, v in e["info"].items()])
                    stage_edges[id_] = e
        sink_node = r["sink"]['node']
        sink_node['call_id'] = str(sink_node['call_id'])
        stage_nodes[sink_node[KW_ID]] = sink_node
        sink_edge = r['sink'][KW_EDGES]
        stage_edges[(sink_edge[KW_SRC], sink_edge[KW_DST])] = sink_edge

        c = r['call_graph']
        for n in c[KW_NODES]:
            id_ = n[KW_ID] = n['call_id'] = str(n[KW_ID])
            api_nodes[id_] = n
        for e in c[KW_EDGES]:
            s, r = str(e[KW_SRC]), str(e[KW_DST])
            if (s, r) not in api_edges:
                e[KW_SRC] = s
                e[KW_DST] = r
                api_edges[(s, r)] = e
    res = {
        "stages": {
            KW_NODES: list(stage_nodes.values()),
            KW_EDGES: list(stage_edges.values()),
        },
        "calls": {
            KW_NODES: list(api_nodes.values()),
            KW_EDGES: list(api_edges.values()),
        }
    }

    return res
