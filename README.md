# Immutable-Service-Bus

- Azure Function App project that demonstrates how to use a lock to prevent processing the same message more than once. Which may be possible when an app scale out to multiple instances:
    - The Service Bus function trigger has an "at least once" message delivery policy:
        - [Azure Functions Service Bus trigger bindings](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-service-bus-trigger?tabs=python-v2%2Cisolated-process%2Cnodejs-v4%2Cextensionv5&pivots=programming-language-python#peeklock-behavior)
        - [Azure Service Bus Messaging - Receive Modes](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-queues-topics-subscriptions#receive-modes)
        - [Azure Functions Idempotent Execution](https://learn.microsoft.com/en-us/azure/azure-functions/functions-idempotent)

    - High-level workflow:
    HTTP trigger function -> Service Bus queue -> Service Bus queue trigger function -> Lock file in Blob Storage
        - HTTP trigger function: Triggered by an HTTP request from a client. The HTTP request should contain the message content. The function then sends a message to the Service Bus queue.
        - Service Bus queue trigger function: Triggered by a Service Bus queue message. 
            - Checks if there is an active lock for the message ID of the message using a blob storage client.
                - If there is an active lock, the current function execution fails due to an exception with the following message:
                "The message ID dbce9dc2685c4997acb05f2067ed29a3 has an active lock lock-dbce9dc2685c4997acb05f2067ed29a3.lock until 2024-02-19 00:24:41.314111"
                - (Acquire lock) If there is an expired lock, then the lock is updated which sets a new expiration time and a new owner (the current function) by updating the blob file's metadata.
                - (Acquire lock) If there is no lock, then a blob file/lock is created for the current function execution. 
            - Releases lock by deleting the blob file.





- TODO: consider lock expiration for current function execution. how should this be handled. 
    - after every complete opperation a check point could be created or appended (maybe to the lock blob file)
    - use Azure functions function execution timeout property to end any execution that has a duration >= 5mins