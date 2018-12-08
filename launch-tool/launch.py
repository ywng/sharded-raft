#!/usr/bin/env python3
from kubernetes import client, config

import argparse
import copy
import os
import subprocess
import sys
import yaml

def find_pods(v1, image):
    """Find pods started by us or at least running raft-peer"""
    ret = v1.list_pod_for_all_namespaces(watch=False)
    def pod_filter(p):
        return p.metadata.namespace == "default" and \
                len(p.spec.containers) == 1 and \
                p.spec.containers[0].image == image
    pods_we_own = filter(pod_filter, ret.items)
    return pods_we_own

def shutdown_pod(v1, name, namespace):
    """Shutdown a single pod"""
    response = v1.delete_namespaced_pod(name, \
            namespace,\
            client.V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy='Foreground')
    response = v1.delete_namespaced_service(name, \
            namespace,\
            client.V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy='Foreground')

def shutdown_pods(v1, pods):
    """Shutdown the given set of pods"""
    for i in pods:
        try:
            shutdown_pod(v1, i.metadata.name, i.metadata.namespace)
        except Exception as e:
            print("Error in killing %s %s"%(i, e), file=sys.stderr)

def get_service(v1, service):
    """Get service spec for service"""
    return v1.list_service_for_all_namespaces(watch=False, field_selector="metadata.name=%s"%service)

def boot_pod(v1, pod_spec, service_spec, name, peers, smpeers, cmd, gid):
    """Boot a single pod"""
    pod_spec = copy.deepcopy(pod_spec)
    # Create a pod spec for this pod.
    pod_spec['metadata']['name'] = name
    pod_spec['metadata']['labels']['app'] = name
    pod_spec['spec']['containers'][0]['ports'][0]['name']="%s-client"%name
    pod_spec['spec']['containers'][0]['ports'][1]['name']="%s-raft"%name
    peers = filter(lambda p: p != name, peers)
    args = [cmd]
    for peer in peers:
        args.append('-peer')
        args.append('%s:3001'%peer)
    for smpeer in smpeers:
        args.append('-sm')
        args.append('%s:3000'%smpeer)
    if gid != -1:
        args.append('-gid')
        args.append(str(gid))
    
    pod_spec['spec']['containers'][0]['command'] = args

    service_spec = copy.deepcopy(service_spec)
    # Create a service spec for this service
    service_spec['metadata']['name'] = name
    service_spec['spec']['selector']['app'] = name
    service_spec['spec']['ports'][0]['targetPort'] = "%s-client"%name
    service_spec['spec']['ports'][1]['targetPort'] = "%s-raft"%name
    try:
        response = v1.create_namespaced_pod('default', pod_spec)
        response = v1.create_namespaced_service('default', service_spec)
    except:
        print("Could not launch pod or service")
        raise

def init():
    """Initialize and get client"""
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return v1

def shutdown(args):
    """Shutdown pods run by us. This might take a while after returning"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    shutdown_pods(v1, pods)

def shutdown_sg(args):
    """Shutdown pods run by us. This might take a while after returning"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    peer = 'peer%d'%args.sg
    pods = list(filter(lambda i: i.metadata.name.startswith(peer), pods))
    shutdown_pods(v1, pods)

def shutdown_sm(args):
    """Shutdown pods run by us. This might take a while after returning"""
    v1 = init()
    pods = find_pods(v1, 'local/shard-master-peer')
    shutdown_pods(v1, pods)

def show(args):
    """Show pods launched by us"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    for pod in pods:
        print("%s"%pod.metadata.name)

def show_sg(args):
    """Show pods launched by us"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    peer = 'peer%d'%args.sg
    pods = list(filter(lambda i: i.metadata.name.startswith(peer), pods))
    for pod in pods:
        print("%s"%pod.metadata.name)

def show_sm(args):
    """Show pods launched by us"""
    v1 = init()
    pods = find_pods(v1, 'local/shard-master-peer')
    for pod in pods:
        print("%s"%pod.metadata.name)

def boot(args):
    """Launch a set of pods"""
    v1 = init()
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        pod_spec = specs[0]
        service_spec = specs[1]
        num_services = args.peers
        peers = ['peer%d-%d'%(args.sg, i) for i in range(num_services)]
        sm_peers = ['sm-peer%d'%i for i in range(args.smpeers)]
        for peer in peers:
            boot_pod(v1, pod_spec, service_spec, peer, peers, sm_peers, 'server', args.sg)


def boot_sm(args):
    """Launch a set of pods"""
    v1 = init()
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        pod_spec = specs[2]
        service_spec = specs[1]
        #print(pod_spec)
        num_services = args.peers
        peers = ['sm-peer%d'% i for i in range(num_services)]
        for peer in peers:
            boot_pod(v1, pod_spec, service_spec, peer, peers, [], 'shardmaster', -1)

def kill(args):
    """Kill selected peer"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    peer = 'peer%d-%d'%(args.sg, args.peer)
    pod = list(filter(lambda i: i.metadata.name == peer, pods))
    if len(pod) != 1:
        sys.exit(1)
    shutdown_pod(v1, pod[0].metadata.name, pod[0].metadata.namespace)

def kill_sm(args):
    """Kill selected peer"""
    v1 = init()
    pods = find_pods(v1, 'local/shard-master-peer')
    peer = 'sm-peer%d'%args.peer
    pod = list(filter(lambda i: i.metadata.name == peer, pods))
    if len(pod) != 1:
        sys.exit(1)
    shutdown_pod(v1, pod[0].metadata.name, pod[0].metadata.namespace)

def kill_sm_all(args):
    """Kill selected peer group"""
    v1 = init()
    pods = find_pods(v1, 'local/shard-master-peer')
    for pod in pods:
        shutdown_pod(v1, pod.metadata.name, pod.metadata.namespace)

def kill_sg(args):
    """Kill selected peer group"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    peer = 'peer%d'%args.sg
    pods = list(filter(lambda i: i.metadata.name.startswith(peer), pods))
    for pod in pods:
        shutdown_pod(v1, pod.metadata.name, pod.metadata.namespace)

def launch(args):
    """Launch an individual peer"""
    v1 = init()
    pods = find_pods(v1, 'local/sharded-raft-peer')
    peers = ['peer%d-%d'%(args.sg, i) for i in range(args.peers) if i != args.peer]
    pod = "peer%d-%d"%(args.sg, args.peer)
    if pod in peers:
        print("%s is already running"%pod, out=sys.stderr)
        sys.exit(1)
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        pod_spec = specs[0]
        service_spec = specs[1]
        sm_peers = ['sm-peer%d'%i for i in range(args.smpeers)]
        boot_pod(v1, pod_spec, service_spec, pod, peers, sm_peers, 'server', args.sg)

def launch_sm(args):
    """Launch an individual peer"""
    v1 = init()
    pods = find_pods(v1, 'local/shard-master-peer')
    peers = list(map(lambda i: i.metadata.name, pods))
    pod = "sm-peer%d"%args.peer
    if pod in peers:
        print("%s is already running"%pod, out=sys.stderr)
        sys.exit(1)
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        pod_spec = specs[2]
        service_spec = specs[1]
        boot_pod(v1, pod_spec, service_spec, pod, peers, [], 'shardmaster', -1)

def get_service_url(args):
    """Get service URL for peer"""
    v1 = init()
    ip = subprocess.run('minikube ip', check=True, stdout=subprocess.PIPE, shell=True).stdout\
            .decode('utf-8').strip()
    svcs = get_service(v1, "peer%d-%d"%(args.sg, args.peer))
    if len(svcs.items) != 1:
        print("Could not find service", file=sys.stderr)
        sys.exit(1)
    svc = svcs.items[0]
    ports = svc.spec.ports
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        service_spec = specs[1]
        for port in ports:
            if port.port == service_spec['spec']['ports'][0]['port']:
                print('%s:%d'%(ip, port.node_port))
                sys.exit(0)
    sys.exit(1)

def get_sm_service_url(args):
    """Get service URL for peer"""
    v1 = init()
    ip = subprocess.run('minikube ip', check=True, stdout=subprocess.PIPE, shell=True).stdout\
            .decode('utf-8').strip()
    svcs = get_service(v1, "sm-peer%d"%args.peer)
    if len(svcs.items) != 1:
        print("Could not find service", file=sys.stderr)
        sys.exit(1)
    svc = svcs.items[0]
    ports = svc.spec.ports
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        service_spec = specs[1]
        for port in ports:
            if port.port == service_spec['spec']['ports'][0]['port']:
                print('%s:%d'%(ip, port.node_port))
                sys.exit(0)
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(prog=sys.argv[0])
    subparsers = parser.add_subparsers(help="sub-command help", dest='command')
    subparsers.required = True
    
    shutdown_parser = subparsers.add_parser("shutdown")
    shutdown_parser.set_defaults(func=shutdown)

    shutdown_sg_parser = subparsers.add_parser("shutdown-sg")
    shutdown_sg_parser.add_argument('sg', type=int, default=0, help='Server group')
    shutdown_sg_parser.set_defaults(func=shutdown_sg)

    shutdown_sm_parser = subparsers.add_parser("shutdown-sm")
    shutdown_sm_parser.set_defaults(func=shutdown_sm)

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func = show)

    list_sg_parser = subparsers.add_parser("list-sg")
    list_sg_parser.add_argument('sg', type=int, default=0, help='Server group')
    list_sg_parser.set_defaults(func = show_sg)

    list_sm_parser = subparsers.add_parser("list-sm")
    list_sm_parser.set_defaults(func = show_sm)

    run_parser = subparsers.add_parser("boot")
    run_parser.add_argument('sg', type=int, default=0, help='Server group')
    run_parser.add_argument('peers', type=int, default=3, help='How many peers?')
    run_parser.add_argument('smpeers', type=int, default=3, help='How many sm peers?')
    run_parser.set_defaults(func = boot)

    run_sm_parser = subparsers.add_parser("boot-sm")
    run_sm_parser.add_argument('peers', type=int, default=3, help='How many peers?')
    run_sm_parser.set_defaults(func = boot_sm)

    kill_parser = subparsers.add_parser("kill")
    kill_parser.add_argument('sg', type=int, default=0, help='Server group')
    kill_parser.add_argument('peer', type=int, help='Which peer should die')
    kill_parser.set_defaults(func = kill)

    kill_sm_parser = subparsers.add_parser("kill-sm")
    kill_sm_parser.add_argument('peer', type=int, help='Which peer should die')
    kill_sm_parser.set_defaults(func = kill_sm)

    kill_sm_all_parser = subparsers.add_parser("kill-sm-all")
    kill_sm_all_parser.set_defaults(func = kill_sm_all)

    kill_sg_parser = subparsers.add_parser("kill-sg")
    kill_sg_parser.add_argument('sg', type=int, default=0, help='Server group')
    kill_sg_parser.set_defaults(func = kill_sg)
    
    kill_parser = subparsers.add_parser("launch")
    kill_parser.add_argument('sg', type=int, default=0, help='Server group')
    kill_parser.add_argument('peer', type=int, help='Which peer should be launched')
    kill_parser.add_argument('peers', type=int, default=3, help='How many peers?')
    kill_parser.add_argument('smpeers', type=int, default=3, help='How many sm peers?')
    kill_parser.set_defaults(func = launch)

    kill_sm_parser = subparsers.add_parser("launch-sm")
    kill_sm_parser.add_argument('peer', type=int, help='Which peer should be launched')
    kill_sm_parser.set_defaults(func = launch_sm)

    svc_parser = subparsers.add_parser("client-url")
    svc_parser.add_argument('sg', type=int, default=0, help='Server group')
    svc_parser.add_argument('peer', type=int, help="Which peer do you need URL for")
    svc_parser.set_defaults(func = get_service_url)

    svc_sm_parser = subparsers.add_parser("sm-client-url")
    svc_sm_parser.add_argument('peer', type=int, help="Which peer do you need URL for")
    svc_sm_parser.set_defaults(func = get_sm_service_url)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
    sys.exit(0)
