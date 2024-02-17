import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import datetime
import os


app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="sbmessage", queue_name="mysbqueue",
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



    
    