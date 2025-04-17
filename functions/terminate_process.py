import platform, subprocess

def terminate_process_using_port():
    os_name = platform.system()
    port_number = 7071

    if os_name == "Windows":
        # Find the PID
        find_pid_command = f"netstat -ano | findstr :{port_number}"
        process = subprocess.Popen(find_pid_command, shell=True, stdout=subprocess.PIPE)
        output, errors = process.communicate()
        output_str = output.decode()
        lines = output_str.splitlines()

        for line in lines:
            if f":{port_number}" in line:
                pid = line.strip().split()[-1]
                # Kill the process
                kill_command = f"taskkill /F /PID {pid}"
                subprocess.run(kill_command, shell=True)
                print(f"Process using port {port_number} (PID {pid}) terminated.")