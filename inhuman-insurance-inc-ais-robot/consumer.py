import requests
from robocorp import workitems
from robocorp.tasks import task


@task
def consume_traffic_data():
    """
    Consumes traffic data work items
    """
    print("consume")
    for item in workitems.inputs:
        traffic_data = item.payload["traffic_data"]
        print(traffic_data)
        if validate_traffic_data(traffic_data):
            status, response_json = post_traffic_data_to_sales(traffic_data)
            if status == 200:
                item.done()
            else:
                item.fail(
                        exception_type="APPLICATION",
                        code="TRAFFIC_DATA_POST_FAILED",
                        message=response_json["message"]
                )
        else:
            print("fail")
            item.fail(
                exception_type="BUSINESS",
                code="INVALID_TRAFFIC_DATA",
                message=item.payload
            )

def validate_traffic_data(data):
    """Returns a bool indicating data is/not valid"""
    # Check country code length
    if len(data["country"]) == 3:
        return True
    else:
        return False
    
def post_traffic_data_to_sales(data):
    """Sends traffic data to the Sales System via API call"""
    url = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"
    response = requests.post(url, json=data)
    return response.status_code, response.json()