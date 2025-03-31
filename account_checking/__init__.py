import datetime, subprocess, platform, os
import logging

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        from .scripts._1_main import main
        main()

        logging.info("Executed successfully")

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
                    return
    
    except Exception as e:
        logging.error(f"Error: {e}")
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
                    return

    return func.HttpResponse("Executed successfully",status_code=200)
