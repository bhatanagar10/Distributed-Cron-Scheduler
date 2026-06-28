import requests
import json

# Define range
i = 51
j = 100

url = "http://localhost:8080/api/jobs"

headers = {
    "Content-Type": "application/json"
}

for job_num in range(i, j + 1):
    payload = {
        "name": f"Test Job {job_num}",
        "targetUrl": "https://3a0cf2fc-7b2c-470d-90d6-f1bea4265731.mock.pstmn.io/test",
        "httpMethod": "GET",
        "cronExpression": "*/1 * * * *"
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload
        )

        print(
            f"Job {job_num}: Status={response.status_code}, "
            f"Response={response.text}"
        )

    except Exception as e:
        print(f"Job {job_num}: Failed - {e}")