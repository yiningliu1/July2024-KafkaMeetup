from typing import Dict, List, Union
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from risingwave.udf import udtf, UdfServer
import datetime
import base64
import struct
import socket

client_options = {"api_endpoint": "us-east4-aiplatform.googleapis.com"}
client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
print("created client")

def predict_custom_trained_model_sample(
    project: str,
    endpoint_id: str,
    instances: Union[Dict, List[Dict]],
    location: str = "us-east4",
    client = client
):
    """
    `instances` can be either a single instance of type dict or a list of instances.
    """

    # Ensure instances is a list
    instances = instances if isinstance(instances, list) else [instances]

    # Convert instances to the required format
    instances = [json_format.ParseDict(instance, Value()) for instance in instances]

    # Define the endpoint path
    endpoint = client.endpoint_path(project=project, location=location, endpoint=endpoint_id)

    # Make the prediction request
    response = client.predict(endpoint=endpoint, instances=instances)

    # Check the predictions
    if hasattr(response, 'predictions'):
        return response.predictions
    else:
        print("No predictions found in the response.")
        return None

def get_predictions(frame):
    dict = {
        "frame": base64.b64encode(frame).decode('ascii'),
    }

    try:
        response = predict_custom_trained_model_sample(
            project="469843768266",
            endpoint_id="4726039625762603008",
            location="us-east4",
            instances=dict
        )
        return response
    except Exception as e:
        print('Error:', e)
        return None

@udtf(input_types=['BYTEA', 'TIMESTAMP'], result_types=['VARCHAR', 'TIMESTAMP'])
def read_plates(frames, ts: datetime.datetime):
    plates = get_predictions(frames)
    for plate in plates:
        try: 
            yield (plate, ts)
        except Exception as e:
            print('Error:', e)
            continue

if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(read_plates)
    server.serve()