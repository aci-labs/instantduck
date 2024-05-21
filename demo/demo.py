import json
import subprocess


def send_data_via_curl(json_file_path, url):
    # Read JSON file
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)

    counter = 0
    # Iterate over each JSON object and send a curl request
    for record in data:
        while counter < 13:
            json_data = json.dumps(record)
            command = [
                "curl",
                "-X",
                "POST",
                url,
                "-H",
                "Content-Type: application/json",
                "-d",
                json_data,
            ]
            subprocess.run(command)
            counter += 1
            break


# Sample usage
json_file_path = "./vehicle_telemetry.json"
url = "http://localhost:5000/ingest?tags=telemetry"
send_data_via_curl(json_file_path, url)
