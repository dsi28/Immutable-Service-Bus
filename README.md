# Immutable-Service-Bus
- TODO: Release Lock function.
    - used for deleting blob lock file when function execution completes
- TODO: consider lock expiration for current function execution. how should this be handled. 
    - after every complete opperation a check point could be created or appended (maybe to the lock blob file)
    - use Azure functions function execution timeout property to end any execution that has a duration >= 5mins