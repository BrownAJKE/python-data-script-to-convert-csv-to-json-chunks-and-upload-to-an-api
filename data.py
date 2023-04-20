import os
import pandas as pd
import json
import requests
import time
from tqdm import tqdm

# define endpoint URL
endpoint_url = 'https://example.com/api/'

# headers
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer Token here"
}

# get list of CSV files in directory
csv_files = [file for file in os.listdir('.') if file.endswith('.csv')]

if len(csv_files) > 0:
    # read CSV files into list of data frames
    data_frames = [pd.read_csv(file) for file in csv_files]

    # concatenate data frames into a single data frame
    merged_data = pd.concat(data_frames, axis=0, ignore_index=True)

    # convert merged data frame to JSON format
    json_data = merged_data.to_json(orient='records')

    # parse JSON data
    json_objects = json.loads(json_data)

    # print total number of records
    print(f"Total Records: {len(json_objects)}")

    # print first row of JSON data as Python object
    print(json_objects[0])

    print(json.loads(json_data)[0])
    # write JSON data to a file
    # with open('merged_data.json', 'w') as f:
    #     f.write(json_data)

     # divide JSON data into chunks of 2000 records
    chunk_size = 2000
    json_chunks = [json_objects[i:i+chunk_size]
        for i in range(0, len(json_objects), 2000)]

    # print sample chunk
    print(json.dumps(json_chunks[0][:5], indent=2))

    # print number of chunks
    print(f"Number of Chunks: {len(json_chunks)}")

    # print number of records in each chunk
    print(f"Records per Chunk: {[len(chunk) for chunk in json_chunks]}")

    # make POST request to endpoint with each chunk of data
    for chunk in tqdm(json_chunks):
        try:

            request_body = {
                "data": chunk
            }

            # send a POST request with the chunk data
            response = requests.post(
                endpoint_url, data=json.dumps(request_body), headers=headers)

            print(f"Response status code: {response.status_code}")
            print(f"Response content: {response.content}")

            # handle the response
            if response.status_code == 200:
                print("POST request successful")
            else:
                print("POST request unsuccessful")

            # set a 1-second delay between each request
            time.sleep(10)
        except Exception as e:
            # handle the error
            print("Error occurred:", str(e))

    print("Data updated successfuly")
else:
    print("No CSV files found in directory.")
