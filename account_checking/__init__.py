import datetime, subprocess, platform, os, logging, azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    from ..functions.terminate_process import terminate_process_using_port

    try:
        from .scripts._0_main import main
        main()
        terminate_process_using_port()

        
    except Exception as err:
        logging.error('Error in main function: %s', err)
        terminate_process_using_port()

    logging.info('Function executed at %s', utc_timestamp)
