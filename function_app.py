import azure.functions as func
import logging

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="mysbqueue",
                               connection="daizquieSBNS_SERVICEBUS") 
def servicebus_queue_trigger(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
    sb_message =  azservicebus.get_body().decode('utf-8')


