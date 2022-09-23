from __future__ import annotations
from typing import Union, Tuple, Optional
from dataclasses import dataclass
import time
import io
import sys
import argparse
import subprocess
import shutil
import yaml
import logging
from pathlib import Path

from cassandra import DriverException
from cassandra.cluster import Cluster, Session, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy # type: ignore
from cassandra.pool import Host # type: ignore

logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout,
        level=logging.DEBUG
        )

@dataclass
class Conf:
    path: Path
    addr: str
    seed: str

def workdir(c: Conf) -> Path:
    return c.path / 'workdir'

def log_path(c: Conf) -> Path:
    return c.path / 'log'

def scylla_conf_dict(c: Conf) -> dict[str, object]:
    return {
        'workdir': str(workdir(c).resolve()),
        'listen_address': c.addr,
        'rpc_address': c.addr,
        'api_address': c.addr,
        'prometheus_address': c.addr,
        'developer_mode': True,
        'skip_wait_for_gossip_to_settle': 0,
        'ring_delay_ms': 0,
        'seed_provider': [{
            'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
            'parameters': [{ 'seeds': f'{c.seed}' }]
        }],
    }

def write_conf(conf: dict[str, object], path: Path):
    with path.open('w') as f:
        yaml.dump(conf, f)

def prepare_node_dir(c: Conf) -> None:
    confdir = c.path / 'conf'
    confdir.mkdir(parents=True, exist_ok=True)
    write_conf(scylla_conf_dict(c), confdir / 'scylla.yaml')
    shutil.rmtree(workdir(c), ignore_errors=True)
    log_path(c).unlink(missing_ok=True)

def run_node(scylla_bin: Path, cwd: Path, log_file: io.TextIOWrapper) -> subprocess.Popen:
    return subprocess.Popen(
        list[Union[str, Path]]([
            scylla_bin,
            '--smp', '2',
            '-m', '1G',
            '--overprovisioned',
        ]),
        stdout=log_file, stderr=subprocess.STDOUT,
        universal_newlines=True, bufsize=1,
        cwd=cwd,
    )

class Node:
    def __init__(self, scylla_bin: Path, c: Conf) -> None:
        self.bin = scylla_bin
        self.config = c
        self.process: Optional[subprocess.Popen] = None
        prepare_node_dir(c)

    def __enter__(self) -> Node:
        self.log_file = log_path(self.config).open('a')
        return self

    def __exit__(self, *exc_details) -> None:
        self.log_file.close()
        if self.process:
            self.process.kill()
            self.process.wait()

    def start(self) -> None:
        assert(self.log_file)
        self.process = run_node(self.bin, self.config.path, self.log_file)

    def stop(self) -> None:
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None

def find_host(s: Session, ip: str) -> Optional[Host]:
    hosts = s.cluster.metadata.all_hosts()
    for host in hosts:
        if str(host.address) == ip:
            return host
    return None

def wait_for_cql_on_session(s: Session, ip: str, deadline: float) -> None:
    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        host = find_host(s, ip)
        if host:
            break
        logging.info(f"Waiting for driver session {s} to learn about host {ip}...")
        time.sleep(1)

    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        try:
            s.execute("select * from system.local", host=host)
        except NoHostAvailable:
            logging.info(f"Waiting for CQL to become available on session {s} for host {ip}...")
        else:
            break
        time.sleep(1)

def wait_for_node(addr: str, deadline: float) -> None:
    policy = WhiteListRoundRobinPolicy([addr])
    prof = ExecutionProfile(load_balancing_policy=policy)
    while True:
        try:
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: prof}, contact_points=[addr], protocol_version=4) as c:
                with c.connect() as session:
                    wait_for_cql_on_session(session, addr, deadline)
        except (NoHostAvailable, DriverException):
            logging.info(f"Waiting for node to become available...")
            time.sleep(1)
            continue
        else:
            break

parser = argparse.ArgumentParser()
parser.add_argument('--scylla-path', type=Path, required=True)
args = parser.parse_args()

root = Path('/tmp/test')
p1 = root / 'node1'; ip1 = '127.0.0.1'
p2 = root / 'node2'; ip2 = '127.0.0.2'

with Node(args.scylla_path, Conf(p1, ip1, ip1)) as n1, \
     Node(args.scylla_path, Conf(p2, ip2, ip1)) as n2:
    logging.info(f"Starting node {ip1}")
    n1.start()
    wait_for_node(ip1, time.time() + 10)
    logging.info(f"Starting node {ip2}")
    n2.start()
    wait_for_node(ip2, time.time() + 10)

    logging.info(f"Nodes started")
    with Cluster(contact_points=[ip1, ip2], protocol_version=4) as c:
        with c.connect() as session:
            wait_for_cql_on_session(session, ip1, time.time() + 10)
            wait_for_cql_on_session(session, ip2, time.time() + 10)
            for i in range(20):
                logging.info(f"Iteration {i}")
                n = [n1, n2][i % 2]
                ip = n.config.addr
                logging.info(f"Stopping node {ip}")
                n.stop()
                logging.info(f"Restarting node {ip}")
                n.start()
                wait_for_node(ip, time.time() + 10)
                wait_for_cql_on_session(session, ip, time.time() + 90)
    logging.info(f"Finished")
