from logging import *
from pyyaml import YAML

CURR_DIR = "."

class ManageRemoteMachine:
    def __init__(self, jumpbox, switch_user, switch_ips, ssh_key_path):
        self.config = config
        self.jumpbox = jumpbox
        self.switch_user = switch_user
        self.switch_ips = switch_ips
        self.ssh_key_path = ssh_key_path
        self.retries = 5
        self.log = PktGenLogger()


    def scp_to_switch(self, dest_ip, file_to_copy, dest_path) -> subprocess.CompletedProcess: 
        scp_cmd = ""
        scp_base = ["scp", "-o", "StrictHostKeyChecking=no"]
        if self.jumpbox:
            scp_base += ["-J", self.jumpbox]
        if self.ssh_key_path:
            scp_base += ["-i", self.ssh_key_path]
        
        num_times = 0
        for num_times in range(0, self.retries):
            try:
                scp_cmd = scp_base + [file_to_copy, f"{node.username}@{dest_ip}:{dest_path}"]
                printable_cmd = " ".join(f"'{arg}'" if " " in arg or ";" in arg else arg for arg in scp_cmd)
                return subprocess.run(scp_cmd, text=True, check=True, capture_output=True, timeout=45)
            except subprocess.CalledProcessError as e:
                num_times += 1
                self.log.error(f"SCP command failed: {scp_cmd}\nStderr: {e.stderr}")
                if "MaxStartups" in e.stderr or "kex_exchange_identification" in str(e.stderr) or "Connection closed by UNKNOWN" in str(e.stderr):
                    time.sleep(2)
                    continue
                else:
                    raise e

    def scp_from_switch(self, src_ip, file_to_copy, dest_path) -> subprocess.CompletedProcess: 
        scp_cmd = ""
        scp_base = ["scp", "-o", "StrictHostKeyChecking=no"]
        if self.jumpbox:
            scp_base += ["-J", self.jumpbox]
        if self.ssh_key_path:
            scp_base += ["-i", self.ssh_key_path]
        
        num_times = 0
        for num_times in range(0, self.retries):
            try:
                scp_cmd = scp_base + [f"{self.switch_user}@{src_ip}:{file_to_copy}", dest_path]
                printable_cmd = " ".join(f"'{arg}'" if " " in arg or ";" in arg else arg for arg in scp_cmd)
                return subprocess.run(scp_cmd, text=True, check=True, capture_output=True, timeout=45)
            except subprocess.CalledProcessError as e:
                num_times += 1
                self.log.error(f"SCP command failed: {scp_cmd}\nStderr: {e.stderr}")
                if "MaxStartups" in e.stderr or "kex_exchange_identification" in str(e.stderr) or "Connection closed by UNKNOWN" in str(e.stderr):
                    time.sleep(2)
                    continue
                else:
                    raise e
    
    def build_ssh_base(self, switch_ip) -> List[str]:
        """Constructs base SSH command injection arrays handling ProxyJumps."""
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=60", "-A", "-R", "8080:codeberg.org:22"]
        if self.jumpbox:
            ssh_cmd += ["-J", self.jumpbox]
        if self.ssh_key_path:
            ssh_cmd += ["-i", self.ssh_key_path]
        ssh_cmd += [f"{self.username}@{self.switch_ip}"]
        return ssh_cmd
    
    def execute_remote_cmd(self, switch_ip, switch_id, cmd: str) -> subprocess.CompletedProcess:
        """Executes a command on a switch passing through the jumpbox tunnel."""
        full_command = self.build_ssh_base(switch_ip) + [cmd]
        printable_cmd = " ".join(f"'{arg}'" if " " in arg or ";" in arg else arg for arg in full_command)
        self.log.info(f"Switch {switch_id} Executing Remote Command:\n  {printable_cmd}")
        num_times = 0
        for num_times in range(0, self.retries):
            try:
                return subprocess.run(full_command, capture_output=True, text=True, check=True, timeout=45)
            except subprocess.CalledProcessError as e:
                num_times += 1
                self.log.error(f"Remote command failed: {printable_cmd}\nStderr: {e.stderr}")
                if "MaxStartups" in e.stderr or "kex_exchange_identification" in e.stderr or "Connection closed by UNKNOWN" in e.stderr:
                    self.log.error(f"Retrying remote command!")
                    time.sleep(2)
                    continue
                else:
                    raise e
    
    def update_pringles_code(self, switch_ip, switch_id, git_hash, p4_name, p4_path, p4_conf_path):
        # Step 1: Pull new github updates from a particular git hash into the pringles repo
        git_cmd = f"git stash && git checkout {git_hash}"
        execute_remote_cmd(switch_ip, switch_id, git_cmd)

        # Step 2: Compile switch SDE
        configure_sde_cmd = f"./configure --prefix=$SDE_INSTALL --with-p4c=bf-p4c --enable-thrift --with-tofino P4_NAME={p4_name} P4_PATH={p4_path} P4_VERSION=p4-16 P4_ARCHITECTURE=tna" 
        compile_cmd = f"{configure_sde_cmd} && make install"
        status = execute_remote_cmd(switch_ip, switch_id, compile_cmd)
        if status is None:
            # what to do here? For now, just notify script user
            return -1

        # Step 3: Update configure file with correct number of pipes 
        yaml = YAML()
        
        p4_conf_name = self.get_conf_name(p4_conf_path)
        self.scp_from_switch(switch_ip, p4_conf_path, CURR_DIR)
        with open(p4_conf_name, "r") as f:
          data = yaml.load(f)
        
        data["p4_devices"]["p4_programs"]["p4_pipelines"][0]["pipe_scope"] = [0]
        data["p4_devices"]["p4_programs"]["p4_pipelines"][1]["pipe_scope"] = [1]
        
        with open(p4_conf_name, "w") as f:
          yaml.dump(data, f)

        self.scp_to_switch(switch_ip, p4_conf_name, p4_conf_path)

