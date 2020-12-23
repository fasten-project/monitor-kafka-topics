import requests
import sys
from datetime import datetime

if len(sys.argv) != 2:
    print("Please specify Flink JobManager address: python3 deploy.py FLINK_ADDRESS")
    sys.exit(1)

flink_address = str(sys.argv[1])


def get_jars(flink_address):
    jars = requests.get(f"http://{flink_address}/jars").json()
    print("-- Uploaded jars -- ")

    for files in jars["files"]:
        print(f"Id: {files['id']}")
        print(f"Name: {files['name']}")
        print(f"Uploaded: {datetime.fromtimestamp(int(str(files['uploaded'])[:-3])).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Main class: {files['entry'][0]['name']}")
        print("--")

def pretty_time_delta(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%s%dd%dh%dm%ds' % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return '%s%dh%dm%ds' % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return '%s%dm%ds' % (sign_string, minutes, seconds)
    else:
        return '%s%ds' % (sign_string, seconds)


def get_running_jobs(flink_address):
    jobs = requests.get(f"http://{flink_address}/jobs/overview").json()
    print("-- Running jobs --")

    for job in jobs["jobs"]:
        print(f"Job ID: {job['jid']}")
        print(f"Name: {job['name']}")
        print(f"State: {job['state']}")
        print(f"Tasks: {job['tasks']['total']}")
        print(f"Duration: {pretty_time_delta(job['duration']/1000)}")
        print("--")


print(f"Deploying Monitoring Jobs on {flink_address}.")

print("-- OVERVIEW --")
get_jars(flink_address)
get_running_jobs(flink_address)