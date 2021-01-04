import requests
import sys
from datetime import datetime
from tqdm import tqdm
import os.path

if len(sys.argv) != 2:
    print("Please specify Flink JobManager address: python3 deploy.py FLINK_ADDRESS")
    sys.exit(1)

flink_address = str(sys.argv[1])


def get_jars(flink_address):
    jars = requests.get(f"http://{flink_address}/jars").json()
    print("-- Uploaded jars -- ")

    all_jars = []
    for files in jars["files"]:
        all_jars.append(files['name'])
        print(f"Id: {files['id']}")
        print(f"Name: {files['name']}")
        print(f"Uploaded: {datetime.fromtimestamp(int(str(files['uploaded'])[:-3])).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Main class: {files['entry'][0]['name']}")
        print("--")

    return all_jars


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

def download_file(url, filename):
    """
    Helper method handling downloading large files from `url` to `filename`. Returns a pointer to `filename`.
    """
    chunkSize = 1024
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        pbar = tqdm( unit="B", total=int( r.headers['Content-Length'] ) )
        for chunk in r.iter_content(chunk_size=chunkSize):
            if chunk: # filter out keep-alive new chunks
                pbar.update (len(chunk))
                f.write(chunk)
    return filename

def get_running_jobs(flink_address):
    jobs = requests.get(f"http://{flink_address}/jobs/overview").json()
    print("-- Running jobs --")

    all_jobs =  []
    for job in jobs["jobs"]:
        all_jobs.append(job['name'])
        print(f"Job ID: {job['jid']}")
        print(f"Name: {job['name']}")
        print(f"State: {job['state']}")
        print(f"Tasks: {job['tasks']['total']}")
        print(f"Duration: {pretty_time_delta(job['duration']/1000)}")
        print("--")

    return all_jobs

def upload_jar(flink_address, jar_url):
    if not os.path.isfile("monitor_job.jar"):
        print("Downloading file: ")
        download_file(jar_url, "../lib/monitor_job.jar")

    print("Upload file: ")
    files = {}
    files["monitor.jar"] = ('monitor_job.jar', open("../lib/monitor_job.jar", 'rb'), 'application/x-java-archive')
    upload = requests.post(f"http://{flink_address}/jars/upload", files=files)

    if upload.json()["status"] == "success":
        print("Upload successful.")
    else:
        print("Upload failed.")
        print(upload.json())

    return upload.json()

def get_last_checkpoint(topic):
    loc = f"/mnt/fasten/monitor-{topic}"
    latest_job = max(all_subdirs = [d for d in os.listdir(loc) if os.path.isdir(d)], key=os.path.getmtime)
    print(latest_job)
    latest_checkpoint = max(all_subdirs = [d for d in os.listdir(latest_job) if os.path.isdir(d)], key=os.path.getmtime)
    print(latest_checkpoint)

def deploy_job(topic, key):
    get_last_checkpoint(topic)

print(f"Deploying Monitoring Jobs on {flink_address}.")

print("-- OVERVIEW --")
jars = get_jars(flink_address)
jobs = get_running_jobs(flink_address)

# Check if we need to upload the jar.
if "monitor_job.jar" not in jars:
    upload_jar(flink_address, "https://github.com/fasten-project/monitor-kafka-topics/raw/1.0/lib/monitor_job.jar")

deploy_job("fasten.GraphDBExtension.out", "meh")