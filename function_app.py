import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import datetime
import os


app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="sbmessage", queue_name=os.getenv('SERVICE_BUS_QUEUE'),
                               connection="daizquieSBNS_SERVICEBUS") 
def servicebus_queue_trigger(sbmessage: func.ServiceBusMessage, context:func.Context):
    message_text =  sbmessage.get_body().decode("utf-8")
    invocation_id = context.invocation_id
    logging.info("Python ServiceBus Queue trigger processed a message: %s", message_text)
    aquire_lock(sbmessage.message_id, invocation_id)



# creates new lock with a duration in minutes
def new_lock(lock_dur):
    lock_expire = datetime.datetime.now()
    lock_expire = lock_expire.replace(minute=(lock_expire.minute + lock_dur) % 60)
    return lock_expire


# check if there is a lock, if not aquire the lock, if there is a lock check if lock is expired.
def aquire_lock(message_id, invocation_id):
    # create blob client
        # TODO: reuse this client between function executions
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AzureWebJobsStorage"))

    # temp test setting a static message id
    if os.getenv('USE_STATIC_MESSAGE_ID'):
        message_id = "1234"
    blob_name = f"lock-{message_id}.lock"

    # get blob even if its not created for the lock file name
    blob_client = blob_service_client.get_blob_client(blob=blob_name, container=os.getenv("LOCKS_CONTAINER"))

    # check if lock blob exsists
    if blob_client.exists():
        logging.warning("lock exists")
        blob_properties = blob_client.get_blob_properties()

        # get lock expiration time and convert it to a string 
        lock_expiration_time = datetime.datetime.strptime(blob_properties.metadata["ExpirationTime"] , "%Y-%m-%d %H:%M:%S.%f")
        logging.warning(f" check \nlock: {lock_expiration_time} \n cur {str(datetime.datetime.now())}")

        # if check if lock expiration is still valid 
        if lock_expiration_time > datetime.datetime.now():
            # throw exception to end current function execution if another function execution owns the lock
            ex_message = f"The message id {message_id} has an active lock {blob_name} until {lock_expiration_time}"
            raise Exception(ex_message)
        else:
            # update lock and owner since the lock is expired in order to give this function execution the lock
            logging.warning('lock is expired')
            new_lock_time = new_lock(5)
            blob_client.set_blob_metadata(metadata={"ExpirationTime":f"{new_lock_time}", "Owner":f"{invocation_id}"})
            blob_properties = blob_client.get_blob_properties()
            new_lock_ex = str(blob_properties.metadata["ExpirationTime"])
            new_owner = str(blob_properties.metadata["Owner"])
            logging.warning(f"new lock expiration time: {new_lock_ex} \n new owner: {new_owner}")
    else:
        # if blob does not exsit, create blob with the following properties:
            # name lock-{service-bus-message-id}.lock
            # ExpirationTime: {expiration-time}
            # Owner: {function-invocation-id}
        logging.warning("not exsists")
        # create expiration time for 5 mins from now
        new_lock_time = new_lock(5)
        blob_client.upload_blob(metadata={"ExpirationTime":f"{new_lock_time}", "Owner": invocation_id}, data="dataTest")



    
    
# http trigger function that sends message to service bus queue
@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    message_data = req.params.get('message')
    if not message_data:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            message_data = req_body.get('message')

    if message_data:
        # send message to service bus. log message id here. 
        try:
            logging.warning('sending')
            await send_service_bus_message(message_data)
            return func.HttpResponse(f"Message sent to service bus queue. This HTTP triggered function executed successfully.")
        except Exception as ex:
            logging.error(str(ex))
            return func.HttpResponse(f"There was an exception when sending message to service bus. This HTTP triggered function executed successfully.", status_code=500)            
    else:
        return func.HttpResponse(
             "no message data was sent with this request. no message sent to service bus",
             status_code=400
        )
    


# create a Service Bus client using the connection string
async def send_service_bus_message(m_data):
    async with ServiceBusClient.from_connection_string(conn_str=os.getenv('daizquieSBNS_SERVICEBUS'), 
                                                       logging_enable=True) as servicebus_client:
        logging.warning('sending 2')
        # Get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=os.getenv('SERVICE_BUS_QUEUE'))
        async with sender:
            # Send one message
            await sender.send_messages(ServiceBusMessage(m_data))
            logging.warning("Sent a single message")