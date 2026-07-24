#!/usr/bin/env python3
"""
Distributed P4 Switch Benchmarking Orchestrator (Remote Laptop Edition)
Author: Expert Systems & Network Engineer
Description: Securely orchestrates P4 switch benchmarks from a local machine
             via an intermediate Jumpbox, with automated telemetry extraction.
             Dynamically generates and provisions per-switch YAML runtime configs.
             Supports nested matrix parameters sweeps for sequential executions.
             Proactively purges stale switch JSON states before each run iteration.

NOTE (pipeline-as-switch refactor, v2):
    A "ring member" is a (switch_id, pipe_id) pair -- ring_topo lists these pairs
    in ring order. HOWEVER, there is still only ONE physical control-plane
    process (run_p4_tests.sh / test.py) and ONE data-plane driver
    (run_switchd.sh) per physical switch, exactly as before. A physical switch
    with both of its pipes acting as ring members gets ONE generated YAML file
    containing a `pipes:` section with one sub-config per active pipe; the
    single control-plane process reads that one file and configures both
    pipes itself, sequentially, within the same run.

    - Hops between two ring members on the SAME switch (its two pipes) never
      leave the chip: they are delivered by addressing a devport that
      physically lives in the target pipe, configured in near-end MAC
      loopback.
    - Hops between ring members on DIFFERENT switches are unchanged: a real
      cable between two real front-panel ports.
"""

import os
import sys
import time
import json
import logging
import argparse
import signal
import subprocess
import yaml
from dataclasses import dataclass, asdict, replace
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import tomllib  # Python 3.11+ built-in
except ImportError:
    import tomli as tomllib
import colorama

# Initialize colorama for cross-platform ANSI support
colorama.init(autoreset=True)


class QuotedStringDumper(yaml.SafeDumper):
    """Forces every string VALUE to be emitted double-quoted, matching the
    original hand-written YAML's convention. Dict keys are left bare/unquoted.
    Ints, floats, bools, and lists are left in their native representation."""

    def ignore_aliases(self, data):
        # Never use YAML anchors/aliases (&id001 / *id001), even when two
        # fields happen to reference the same underlying Python list object
        # (e.g. ports_to_ring_members shared across a switch's pipes). Always
        # write the full value out plainly instead.
        return True


def _quoted_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')


def _bare_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style=None)


def _represent_mapping_bare_keys(dumper, tag, mapping, flow_style=None):
    """Copy of yaml.Representer.represent_mapping, except dict KEYS that are
    strings always go through the bare (unquoted) representer, regardless of
    the representer registered for str values."""
    value = []
    node = yaml.MappingNode(tag, value, flow_style=flow_style)
    if dumper.alias_key is not None:
        dumper.represented_objects[dumper.alias_key] = node
    best_style = True
    if hasattr(mapping, 'items'):
        mapping = list(mapping.items())
    for item_key, item_value in mapping:
        node_key = _bare_str_representer(dumper, item_key) if isinstance(item_key, str) else dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)
        if not (isinstance(node_key, yaml.ScalarNode) and not node_key.style):
            best_style = False
        if not (isinstance(node_value, yaml.ScalarNode) and not node_value.style):
            best_style = False
        value.append((node_key, node_value))
    if flow_style is None:
        if dumper.default_flow_style is not None:
            node.flow_style = dumper.default_flow_style
        else:
            node.flow_style = best_style
    return node


QuotedStringDumper.add_representer(str, _quoted_str_representer)
QuotedStringDumper.represent_mapping = _represent_mapping_bare_keys

class ColoredFormatter(logging.Formatter):
    """Custom formatter to inject ANSI colors into specific log levels."""
    COLORS = {
        'INFO': colorama.Fore.GREEN + '[INFO]' + colorama.Style.RESET_ALL,
        'WARNING': colorama.Fore.YELLOW + '[WARN]' + colorama.Style.RESET_ALL,
        'ERROR': colorama.Fore.RED + '[ERROR]' + colorama.Style.RESET_ALL,
        'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE + '[CRITICAL]' + colorama.Style.RESET_ALL
    }

    def format(self, record):
        levelname = record.levelname
        status_tag = self.COLORS.get(levelname, f"[{levelname}]")
        log_fmt = f"%(asctime)s {status_tag} [%(node)s] %(message)s"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger("P4RemoteOrchestrator")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColoredFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class NodeLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs['extra'] = {'node': self.extra.get('node', 'LAPTOP-COORD')}
        return msg, kwargs

log = NodeLoggerAdapter(logger, {})

# --- Data Structures ---

@dataclass
class ExperimentConfig:
    # Each entry is [switch_id, pipe_id], IN RING ORDER (matches the topology diagram).
    # Used only to compute each pipe's next-hop port -- NOT used to decide how many
    # physical processes/nodes to launch (that's still one per physical switch).
    ring_topo: List[List[int]]
    # Indexed as [switch_id][target_switch_id] -> devport on switch_id with a cable
    # running to target_switch_id. When target_switch_id == switch_id, this is an
    # unused sentinel (0) -- that case is handled by ports_to_pipelines_per_switch
    # instead, since the port to use then depends on WHICH PIPE you're reaching,
    # not on the switch as a whole.
    ports_to_ring_members: List[List[int]]
    # Indexed as [switch_id][pipe_id] -> the devport that pipe_id uses to receive
    # traffic sent to it from WITHIN this same switch (i.e. the intra-chip relay
    # port for that pipe). Used for the same-switch hop case in ring routing.
    ports_to_pipelines_per_switch: List[List[int]]
    loopback_ports: List[List[List[int]]]
    switch_ports: List[List[List[int]]]
    total_recirc_ports: List[int]  # Converted from int to List[int]
    # Which PREFIX LENGTHS of ring_topo to test, e.g. [1, 2, 4] tests
    # ring_topo[:1], ring_topo[:2], ring_topo[:4] as three separate runs.
    # Leave unset (None/empty) to automatically test every prefix length
    # from 1 through len(ring_topo).
    ring_sizes_to_test: Optional[List[int]] = None
    duration: int = 30
    jumpbox: Optional[str] = None
    primary_switch_id: int = 3
    primary_pipe_id: int = 0
    payload_size: int = 100
    acks_required: int = 1
    sde_path: str = "/root/bf-sde-9.4.0/"
    p4_program_name: str = "sequencing_only"
    p4_program_dir: str = "/root/pringles/code/p4/pktgen_tests"
    remote_json_dir: str = "/root"
    local_results_dir: str = "/home/mic/Programming/PhD/pringles/experiments/pktgen_tests/results"
    nsperpkt: List[int] = None

@dataclass
class SwitchNode:
    switch_id: int
    mgmt_ip: str
    username: str
    ssh_key_path: Optional[str] = None
    is_primary: bool = False
    dp_pid: Optional[int] = None
    cp_pid: Optional[int] = None


# --- Core Orchestration Engine ---

class RemoteBenchmarkOrchestrator:
    def __init__(self, config: ExperimentConfig, nodes: List[SwitchNode], current_nsperpkt: int, current_recirc: int, base_results_dir: str, sweep_idx: int):
        self.config = config
        self.nodes = nodes
        self.current_nsperpkt = current_nsperpkt
        self.current_recirc = current_recirc  # Track target recirculation value for this run iteration
        self.ring_size = len(config.ring_topo)  # config passed in is already truncated to this run's prefix
        self.config_json_path = "/tmp/bench_config.json"
        self.remote_yaml_filename = "switch_config.yaml"
        self.remote_yaml_path = f"/tmp/{self.remote_yaml_filename}"
        self.local_run_dir = base_results_dir
        self.sweep_idx = sweep_idx

        with open(self.config_json_path, 'w') as f:
            json.dump(asdict(self.config), f, indent=4)

    def _build_ssh_base(self, node: SwitchNode) -> List[str]:
        """Constructs base SSH command injection arrays handling ProxyJumps."""
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10", "-A"]
        if self.config.jumpbox:
            ssh_cmd += ["-J", self.config.jumpbox]
        if node.ssh_key_path:
            ssh_cmd += ["-i", node.ssh_key_path]
        ssh_cmd += [f"{node.username}@{node.mgmt_ip}"]
        return ssh_cmd

    def _execute_remote_cmd(self, node: SwitchNode, cmd: str) -> subprocess.CompletedProcess:
        """Executes a command on a switch passing through the jumpbox tunnel."""
        full_command = self._build_ssh_base(node) + [cmd]
        printable_cmd = " ".join(f"'{arg}'" if " " in arg or ";" in arg else arg for arg in full_command)
        NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"}).info(f"Executing Remote Command:\n  {printable_cmd}")
        try:
            return subprocess.run(full_command, capture_output=True, text=True, check=True, timeout=45)
        except subprocess.CalledProcessError as e:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            node_log.error(f"Remote command failed: {cmd}\nStderr: {e.stderr}")
            raise e

    def purge_remote_telemetry_footprints(self):
        """Proactively discovers and clears prior JSON artifacts from the target switch environment."""
        primary_node = next((n for n in self.nodes if n.is_primary), None)
        if not primary_node:
            return

        node_log = NodeLoggerAdapter(logger, {'node': f"Primary-Switch-{primary_node.switch_id}"})
        node_log.info(f"Purging legacy JSON metrics from directory: {self.config.remote_json_dir} before starting...")

        clear_cmd = f"find {self.config.remote_json_dir} -maxdepth 1 -name '*.json' ! -name 'bench_config.json' -exec rm -f {{}} +"
        try:
            self._execute_remote_cmd(primary_node, clear_cmd)
            node_log.info("Remote environment reset complete. Discovered output structures neutralized.")
        except Exception as e:
            node_log.warning(f"Non-fatal exception caught clearing remote JSON workspace: {str(e)}")

    def _active_pipes_for_switch(self, switch_id: int) -> List[int]:
        """Which of this switch's pipes actually appear as ring members."""
        return [pipe for (sw, pipe) in self.config.ring_topo if sw == switch_id]

    def _build_pipe_entry(self, node_log, switch_id: int, pipe_id: int, cntrl_port_override: Optional[tuple] = None) -> dict:
        """Builds the per-pipe sub-config dict embedded in this switch's single YAML.

        If cntrl_port_override is given as (cntrl_port, cntrl_port_is_loopback), it's
        used directly instead of computing this pipe's own next-hop in the ring --
        for a pipe that isn't actually part of this run's ring_topo, but whose tables
        still need to be set up on the ASIC regardless (both MyIngress0/MyIngress1
        need SOME valid configuration). Everything else (loopback_ports, switch_ports,
        ports_to_ring_members, recirc truncation) is still built normally."""
        num_ring_members = len(self.config.ring_topo)

        if switch_id < len(self.config.loopback_ports) and pipe_id < len(self.config.loopback_ports[switch_id]):
            active_loopback_ports = list(self.config.loopback_ports[switch_id][pipe_id])
            active_switch_ports = self.config.switch_ports[switch_id][pipe_id]
        else:
            node_log.warning(f"No specific loopback array mapped for switch {switch_id} pipe {pipe_id}. Defaulting to empty array.")
            active_loopback_ports = []
            active_switch_ports = []

        if switch_id < len(self.config.ports_to_ring_members):
            active_ring_ports = self.config.ports_to_ring_members[switch_id]
        else:
            node_log.warning(f"No ports_to_ring_members entry for switch {switch_id}. Defaulting to empty array.")
            active_ring_ports = []

        is_designated_primary = (switch_id == self.config.primary_switch_id)

        # Feature Addition: If this is the designated primary (switch, pipe), apply
        # the current sweep iteration's recirc port subset. TODO: What does this mean?
        if is_designated_primary:
            active_loopback_ports = active_loopback_ports[:self.current_recirc]
            node_log.info(f"Primary pipe (switch {switch_id}, pipe {pipe_id}) configured with dynamic "
                           f"loopback port subset: {active_loopback_ports}")

        if cntrl_port_override is not None:
            active_cntrl_port, cntrl_port_is_loopback = cntrl_port_override
            node_log.info(f"[Switch {switch_id} Pipe {pipe_id}] Not part of this run's ring_topo -- "
                           f"reusing active pipe's cntrl_port {active_cntrl_port} "
                           f"(loopback={cntrl_port_is_loopback}) so its tables still get configured.")
        else:
            try:
                idx = self.config.ring_topo.index([switch_id, pipe_id])
            except ValueError:
                print(f"[{switch_id}, {pipe_id}] is not in the ring topology array")
                sys.exit()

            next_switch_id, next_pipe_id = self.config.ring_topo[(idx + 1) % num_ring_members]

            if next_switch_id == switch_id:
                # Intra-chip hop: next ring member is the OTHER pipe on this SAME switch.
                # The port to use depends on which PIPE we're reaching, not the switch
                # as a whole -- looked up in ports_to_pipelines_per_switch, keyed by the
                # NEXT pipe's id (i.e. "the port that pipe uses to receive traffic sent
                # to it from within this switch"). This port MUST be configured in
                # loopback mode, since the bits never leave the ASIC.
                active_cntrl_port = self.config.ports_to_pipelines_per_switch[switch_id][next_pipe_id]
                cntrl_port_is_loopback = True
                node_log.info(f"[Switch {switch_id} Pipe {pipe_id}] Next hop is intra-chip (-> pipe {next_pipe_id}); "
                               f"using relay port {active_cntrl_port} (loopback)")
            else:
                # Inter-switch hop: a real cable to a real front-panel port on another
                # switch. This port must NOT be configured in loopback mode.
                active_cntrl_port = active_ring_ports[next_switch_id]
                cntrl_port_is_loopback = False
                node_log.info(f"[Switch {switch_id} Pipe {pipe_id}] Next hop is inter-switch (-> switch {next_switch_id}); "
                               f"using cable port {active_cntrl_port} (no loopback)")

        return {
            "pipe_id": pipe_id,
            "cntrl_port": active_cntrl_port,
            "cntrl_port_is_loopback": cntrl_port_is_loopback,
            "loopback_ports": active_loopback_ports,
            "switch_ports": active_switch_ports,
            "ports_to_ring_members": active_ring_ports,
            "total_recirc_ports": self.current_recirc,

        }

    def _generate_and_copy_configs(self, node: SwitchNode):
        """Generates a single switch-wide YAML (with a nested per-pipe section), then pushes it via SCP."""
        node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
        num_ring_members = len(self.config.ring_topo)

        active_pipes = self._active_pipes_for_switch(node.switch_id)
        if not active_pipes:
            node_log.warning(f"Switch {node.switch_id} has no pipes present in ring_topo -- nothing to configure!")

        is_designated_primary = (node.switch_id == self.config.primary_switch_id)

        # Both pipes' tables must be set up on the ASIC regardless of how many are
        # actually part of this run's ring topology. For a pipe that IS in ring_topo,
        # build its entry normally (real next-hop routing). For a pipe that ISN'T,
        # there's no real next hop to compute -- reuse whichever pipe on this switch
        # IS active's cntrl_port/cntrl_port_is_loopback instead, so it still gets a
        # valid, harmless configuration rather than a placeholder that could crash
        # port setup.
        pipes_cfg = [None, None]
        for pipe_id in active_pipes:
            pipes_cfg[pipe_id] = self._build_pipe_entry(node_log, node.switch_id, pipe_id)

        reference_entry = next((e for e in pipes_cfg if e is not None), None)
        for pipe_id in (0, 1):
            if pipes_cfg[pipe_id] is None:
                if reference_entry is None:
                    node_log.warning(f"Switch {node.switch_id} has no active pipe to source a reference "
                                      f"cntrl_port from for pipe {pipe_id} -- leaving it as an unused default.")
                    override = (0, False)
                else:
                    override = (reference_entry['cntrl_port'], reference_entry['cntrl_port_is_loopback'])
                pipes_cfg[pipe_id] = self._build_pipe_entry(
                    node_log, node.switch_id, pipe_id, cntrl_port_override=override
                )

        config_dict = {
            "num_switches": num_ring_members,
            "cntrl_timeout": 10,
            "meta_circulate": 1,
            "out_cntrl": 1,
            "in_cntrl": 1,
            "cpu_interface": "enp5s0",
            "cpu_port": 192,
            "start_view": 1,
            "device_number": 0,
            "size_of_ring": num_ring_members,
            "port_fec": "BF_FEC_TYP_NONE",
            "port_speed": "BF_SPEED_100G",
            "loopback_mode": "BF_LPBK_MAC_NEAR",
            "ack_threshold": self.config.acks_required,
            "primary_switch_id": self.config.primary_switch_id,
            "primary_pipe_id": self.config.primary_pipe_id,
            "payload_size": self.config.payload_size,
            "duration": self.config.duration,
            "sde_path": self.config.sde_path,
            "p4_program_name": self.config.p4_program_name,
            "p4_program_dir": self.config.p4_program_dir,
            "switch_id": node.switch_id,
            "is_primary": node.is_primary,
            "switch_ip": node.mgmt_ip,
            "nsperpkt": self.current_nsperpkt,
            "send_cntrl_pkt": 1 if is_designated_primary else 0,
            "pipes": pipes_cfg,
        }

        yaml_content = "---\n# Automatically generated by P4 Switch Benchmarking Orchestrator\n" + \
            yaml.dump(config_dict, Dumper=QuotedStringDumper, default_flow_style=None, sort_keys=False)

        local_yaml_staging = f"/tmp/switch_test_config_{node.switch_id}.yaml"
        with open(local_yaml_staging, "w") as y_file:
            y_file.write(yaml_content)

        scp_base = ["scp", "-o", "StrictHostKeyChecking=no"]
        if self.config.jumpbox:
            scp_base += ["-J", self.config.jumpbox]
        if node.ssh_key_path:
            scp_base += ["-i", node.ssh_key_path]

        scp_yaml_cmd = scp_base + [local_yaml_staging, f"{node.username}@{node.mgmt_ip}:{self.remote_yaml_path}"]
        subprocess.run(scp_yaml_cmd, check=True, capture_output=True, timeout=30)
        node_log.info(f"Synchronized configuration payload to remote destination: {self.remote_yaml_path}")

        scp_json_cmd = scp_base + [self.config_json_path, f"{node.username}@{node.mgmt_ip}:{self.config_json_path}"]
        subprocess.run(scp_json_cmd, check=True, capture_output=True, timeout=30)

        try:
            os.remove(local_yaml_staging)
        except OSError:
            pass

    def initialize_environment(self):
        log.info("Sweeping cluster infrastructure for stale switch runtime environments...")
        for node in self.nodes:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            try:
                node_log.info("Neutralizing stale data/control plane runtime processes...")
                self._execute_remote_cmd(node, "killall run_switchd.sh killall bf_switchd killall run_p4_tests.sh 2>/dev/null || true")
            except Exception:
                pass

        self.purge_remote_telemetry_footprints()

        log.info("Phase 1: Initializing Remote Clusters via Jumpbox Proxy...")
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            futures = [executor.submit(self._generate_and_copy_configs, node) for node in self.nodes]
            for future in as_completed(futures):
                future.result()

        log.info("Launching data plane switch daemons across remote cluster...")
        for idx, node in enumerate(self.nodes):
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            dp_cmd = (
                f"nohup {self.config.sde_path}/run_switchd.sh "
                f"--arch tofino "
                f"-p {self.config.p4_program_name} "
                f"-c /root/pringles/code/p4/pktgen_tests/sequencing_only_2pipe.conf" # TODO need to parameterize this
                f"> /tmp/p4_dataplane.log 2>&1 & echo $!"
            )
            res = self._execute_remote_cmd(node, dp_cmd)
            node.dp_pid = int(res.stdout.strip())
            node_log.info(f"Data plane active in background. Tracked Remote PID: {node.dp_pid}")
            if idx < len(self.nodes) - 1:
                node_log.info("Pacing activation cycle: Holding 15 seconds for target ASIC initialization...")
                time.sleep(15)

        log.info("Executing driver stability health checks across active nodes...")
        for node in self.nodes:
            self._execute_remote_cmd(node, f"kill -0 {node.dp_pid}")
            NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"}).info("Data plane verified stable.")

        log.info("Deploying control plane rule engines...")
        for node in self.nodes:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            try:
                check_res = self._execute_remote_cmd(node, f"ls -l {self.remote_yaml_path}")
                node_log.info(f"Verified remote configuration payload file presence:\n  {check_res.stdout.strip()}")
            except Exception:
                node_log.warning(f"Configuration file {self.remote_yaml_path} was NOT found or is unreadable right now!")

            cp_cmd = (
                f"nohup {self.config.sde_path}/run_p4_tests.sh "
                f"--arch tofino "
                f"-p {self.config.p4_program_name} "
                f"-t {self.config.p4_program_dir} "
                f"> /tmp/p4_controlplane.log 2>&1 & echo $!"
            )
            res = self._execute_remote_cmd(node, cp_cmd)
            node.cp_pid = int(res.stdout.strip())
            node_log.info(f"Control plane runtime deployed. Tracked Remote PID: {node.cp_pid}")

        log.info("Executing control plane stability health checks (catching immediate crash-on-launch)...")
        time.sleep(3)  # give the process a moment to fail fast if it's going to fail at all
        for node in self.nodes:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            try:
                self._execute_remote_cmd(node, f"kill -0 {node.cp_pid}")
                node_log.info("Control plane verified stable.")
            except Exception:
                node_log.critical(f"Control plane PID {node.cp_pid} is NOT running shortly after launch! "
                                   f"It likely crashed on startup -- check /tmp/p4_controlplane.log on this "
                                   f"machine before trusting this run's results.")

    def run_experiment(self):
        log.info(f"Phase 2: Core Execution Loop Active. Runtime: {self.config.duration}s")
        elapsed = 0
        while elapsed < self.config.duration:
            time.sleep(5)
            elapsed += 5
            log.info(f"Telemetry Gathering Active... ({elapsed}/{self.config.duration}s)")
        log.info("Wait to collect all the JSON and LOG files...")
        time.sleep(20)
        log.info("Target execution duration met successfully.")

    def collect_telemetry_data(self):
        log.info("Scanning for experimental performance JSON footprints to pull back from every ring member...")
        find_cmd = f"find {self.config.remote_json_dir} -maxdepth 1 -name '*.json' ! -name 'bench_config.json'"

        for node in self.nodes:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            try:
                res = self._execute_remote_cmd(node, find_cmd)
                target_files = res.stdout.strip().split()

                if not target_files or target_files == ['']:
                    node_log.warning("No performance output JSON metrics discovered on target filesystem.")
                    continue

                scp_pull = ["scp", "-o", "StrictHostKeyChecking=no"]
                if self.config.jumpbox:
                    scp_pull += ["-J", self.config.jumpbox]
                if node.ssh_key_path:
                    scp_pull += ["-i", node.ssh_key_path]

                for remote_file in target_files:
                    # test.py itself names each pipe's output distinctly (e.g.
                    # pipeN_<nsperpkt>_nsperpkt.json), so this prefix only needs to
                    # identify the switch and experiment index, not the pipe.
                    filename = os.path.basename(remote_file)
                    mapped_filename = f"switch_{node.switch_id}_ringsize_{self.ring_size}_experiment_{self.sweep_idx}_{filename}"
                    local_dest = os.path.join(self.local_run_dir, mapped_filename)

                    node_log.info(f"Downloading telemetry {filename} back to laptop tracking workspace...")
                    scp_cmd = scp_pull + [f"{node.username}@{node.mgmt_ip}:{remote_file}", local_dest]
                    subprocess.run(scp_cmd, check=True, capture_output=True, timeout=30)

                    self._execute_remote_cmd(node, f"rm -f {remote_file}")

                node_log.info(f"Successfully moved custom prefixed telemetry results to: {self.local_run_dir}")
            except Exception as e:
                node_log.error(f"Failed pulling back JSON file footprints: {str(e)}")

    def collect_log_files(self):
        log.info("Initiating post-run runtime log extraction cycle...")
        for node in self.nodes:
            node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
            find_cmd = "find /tmp -maxdepth 1 -name '*.log'"
            try:
                res = self._execute_remote_cmd(node, find_cmd)
                target_logs = res.stdout.strip().split()

                if not target_logs or target_logs == ['']:
                    continue

                scp_pull_base = ["scp", "-o", "StrictHostKeyChecking=no"]
                if self.config.jumpbox:
                    scp_pull_base += ["-J", self.config.jumpbox]
                if node.ssh_key_path:
                    scp_pull_base += ["-i", node.ssh_key_path]

                for remote_log in target_logs:
                    filename = f"switch_{node.switch_id}_ringsize_{self.ring_size}_experiment_{self.sweep_idx}_{os.path.basename(remote_log)}"
                    local_dest = os.path.join(self.local_run_dir, filename)

                    node_log.info(f"Recovering trace log metrics ({os.path.basename(remote_log)}) -> Laptop tracking folder")
                    scp_cmd = scp_pull_base + [f"{node.username}@{node.mgmt_ip}:{remote_log}", local_dest]
                    subprocess.run(scp_cmd, check=True, capture_output=True, timeout=30)

                    self._execute_remote_cmd(node, f"rm -f {remote_log}")

            except Exception as e:
                node_log.warning(f"Log retrieval operations encountered a fault or was skipped: {str(e)}")

    def _kill_and_confirm_exit(self, node: SwitchNode, pid: int, label: str, timeout_s: int = 15):
        """Sends SIGTERM, then actively polls (via `kill -0`, which works for ANY pid
        system-wide, unlike `wait` -- which only works on child processes of the
        CURRENT shell, and a fresh SSH session's shell never has this pid as a child)
        until the process is confirmed gone or the timeout elapses. Falls back to
        SIGKILL if it's still alive after the timeout."""
        node_log = NodeLoggerAdapter(logger, {'node': f"Switch-{node.switch_id}"})
        try:
            self._execute_remote_cmd(node, f"kill -15 {pid} 2>/dev/null || true")
        except Exception:
            pass

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                self._execute_remote_cmd(node, f"kill -0 {pid}")
                time.sleep(1)  # still alive, keep polling
            except Exception:
                node_log.info(f"{label} PID {pid} confirmed exited.")
                return
        node_log.warning(f"{label} PID {pid} still alive {timeout_s}s after SIGTERM -- sending SIGKILL.")
        try:
            self._execute_remote_cmd(node, f"kill -9 {pid} 2>/dev/null || true")
            time.sleep(2)
        except Exception:
            pass

    def teardown_cluster(self):
        log.info("Phase 3: Initiating Graceful Cooldown and Distributed Node Reset...")
        time.sleep(5)

        for node in self.nodes:
            if node.cp_pid:
                self._kill_and_confirm_exit(node, node.cp_pid, "Control Plane")

            if node.dp_pid:
                self._kill_and_confirm_exit(node, node.dp_pid, "Data Plane ASIC Driver")

        log.info("Allowing a brief settle period for ASIC/driver state to fully release before the next run...")
        time.sleep(10)
        log.info("Remote environment cleanup completed. ASIC lines neutralized successfully.")


# --- Main Execution Interface & Exception Trapping ---

def main():
    config_filename = "orchestrator_config.toml"
    if not os.path.exists(config_filename):
        log.critical(f"Configuration file '{config_filename}' not found in current working directory!")
        sys.exit(1)

    with open(config_filename, "rb") as f:
        toml_config = tomllib.load(f)

    config = ExperimentConfig(**toml_config)

    # Standardize input type constraints for nsperpkt tracking lists
    if config.nsperpkt is None:
        nsperpkt_list = [1000000000]
    elif isinstance(config.nsperpkt, int):
        nsperpkt_list = [config.nsperpkt]
    else:
        nsperpkt_list = config.nsperpkt

    run_timestamp = time.strftime("%Y%m%d_%H%M%S")
    base_results_dir = os.path.join(config.local_results_dir, run_timestamp)
    os.makedirs(base_results_dir, exist_ok=True)
    log.info(f"Target execution directory suite initialized: {base_results_dir}")

    switch_inventory = [
        {"switch_id": 0, "mgmt_ip": "10.229.49.3", "username": "root", "ssh_key_path": None},
        {"switch_id": 1, "mgmt_ip": "10.229.49.5", "username": "root", "ssh_key_path": None},
        {"switch_id": 2, "mgmt_ip": "10.229.49.7", "username": "root", "ssh_key_path": None},
        {"switch_id": 3, "mgmt_ip": "10.229.49.9", "username": "root", "ssh_key_path": None},
    ]

    active_orchestrator = None

    def laptop_sig_handler(sig, frame):
        log.warning("\nLocal interruption caught! Deploying emergency cluster crash-stop sequence...")
        if active_orchestrator:
            active_orchestrator.collect_log_files()
            active_orchestrator.teardown_cluster()
        sys.exit(128 + sig)

    signal.signal(signal.SIGINT, laptop_sig_handler)
    signal.signal(signal.SIGTERM, laptop_sig_handler)

    # Core Parameter Matrix Sweep (Nested Loop for matrix combinations)
    ring_sizes = config.ring_sizes_to_test or list(range(1, len(config.ring_topo) + 1))
    for ring_size in ring_sizes:
        if ring_size < 1 or ring_size > len(config.ring_topo):
            log.critical(f"ring_size {ring_size} is out of range for a ring_topo of length "
                         f"{len(config.ring_topo)} -- skipping.")
            continue

        # Truncate ring_topo to its first `ring_size` entries for this run. Everything else
        # (ports_to_ring_members, loopback_ports, switch_ports) stays indexed by [switch][pipe],
        # not by ring position, so it needs no truncation -- only ring_topo itself does.
        truncated_ring_topo = config.ring_topo[:ring_size]
        config_for_run = replace(config, ring_topo=truncated_ring_topo)

        sweep_idx = 0
        for current_recirc in config_for_run.total_recirc_ports:
            for current_nsperpkt in nsperpkt_list:
                log.info(f"\n==========================================================================")
                log.info(f" STARTING EXPERIMENT RUN {sweep_idx} (ring_size = {ring_size}, "
                         f"recirc = {current_recirc}, nsperpkt = {current_nsperpkt})")
                log.info(f" Active ring members this run: {truncated_ring_topo}")
                log.info(f"==========================================================================")

                # One node per PHYSICAL SWITCH -- a switch is included if either of its
                # pipes appears anywhere in this run's truncated ring_topo. Its single
                # control-plane process will configure whichever of its pipes are active.
                switches_in_ring = {sw for (sw, _pipe) in config_for_run.ring_topo}

                nodes = []
                for s in switch_inventory:
                    if s["switch_id"] not in switches_in_ring:
                        continue
                    snode = SwitchNode(
                        switch_id=s["switch_id"],
                        mgmt_ip=s["mgmt_ip"],
                        username=s["username"],
                        ssh_key_path=s["ssh_key_path"],
                        is_primary=(s["switch_id"] == config_for_run.primary_switch_id)
                    )
                    nodes.append(snode)

                if len(nodes) == 0:
                    log.critical("No valid switch nodes matching topology! Aborting loop...")
                    break

                # Safety net: the configured primary_switch_id might not be part of this
                # ring_size's prefix (e.g. primary is ring_topo[1] but ring_size=1 only
                # includes ring_topo[0]). Without a primary, telemetry collection has
                # nothing to pull from -- fall back to the first node in the ring so
                # results are still collected, but say so loudly.
                if not any(n.is_primary for n in nodes):
                    nodes[0].is_primary = True
                    log.warning(f"Configured primary_switch_id={config_for_run.primary_switch_id} is not part "
                                f"of this ring_size={ring_size} prefix. Falling back to switch "
                                f"{nodes[0].switch_id} as primary for telemetry collection purposes.")

                # Instantiates orchestrator with explicit target configuration states
                orchestrator = RemoteBenchmarkOrchestrator(
                    config_for_run, nodes, current_nsperpkt, current_recirc, base_results_dir, sweep_idx
                )
                active_orchestrator = orchestrator

                try:
                    orchestrator.initialize_environment()
                    orchestrator.run_experiment()
                    orchestrator.collect_telemetry_data()
                except Exception as fatal_err:
                    log.critical(f"Experiment iteration failed with runtime error: {str(fatal_err)}")
                finally:
                    orchestrator.collect_log_files()
                    orchestrator.teardown_cluster()

                sweep_idx += 1

    log.info(f"\n[SUCCESS] Entire configuration sweep matrix completed. Check items under: {base_results_dir}")

if __name__ == "__main__":
    main()
