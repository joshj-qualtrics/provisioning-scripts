import requests

api_url = "http://odo-public-api.corp.qualtrics.com/odo-api/brand/provisioningtest"

auth_token = "092f9227-7198-4888-9552-00986d79f225"

headers = {
    "Authorization": f"Bearer {auth_token}"
}

try:
    # Make a GET request, including the headers dictionary
    response = requests.get(api_url, headers=headers)

    # Raise an exception for bad status codes (4xx or 5xx)
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()

    # Print the data
    print("API Call Successful! 😊")
    print("-----------------------")
    print(data)

except requests.exceptions.RequestException as e:
    # Handle any errors that occur during the request
    print(f"An error occurred: {e}")
except requests.exceptions.HTTPError as e:
    # Handle specific HTTP errors, like 401 Unauthorized
    print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
    print("Check your authorization token and permissions.")