# NMOS Node API

Package providing a basic NMOS Node API implementation. It takes the form of a "Node Facade" which accepts data from private back-end data providers. The version of the service on this branch runs a "mock" service that presents a faked node on the API. Users wishing to use the API without this should use the Master branch.

## Installing with Python

Before installing this library please make sure you have installed the [NMOS Common Library](https://github.com/bbc/nmos-common), on which this API depends.

```
pip install setuptools
sudo python setup.py install
```

## Running the Node Facade

If you wish the Node Facade to run automatically as a service a symbolic link must be added to the system. In order to do this, run the following command in bash.

```bash
    ln -s /lib/systemd/system/nmosnode.service /etc/systemd/system/multi-user.target.wants/nmosnode.service
```

If you wish to start and stop the facade manually it can be started and stopped using a Python script. The two scripts below show how to do this in a blocking and non-blocking way.

### Non-blocking

Run the following script to start the Node Facade in a non-blocking manner, and then stop it again at a later point:

```Python
    from nmosnode.nodefacadeservice import NodeFacadeService
    
    service = NodeFacadeService()
    service.start()
    
    # Do something else until ready to stop
    
    service.stop()
```

### Blocking

It is also possible to run Node Facade in a blocking manner:

```Python
    from nmosnode.nodefacadeservice import NodeFacadeService
    
    service = NodeFacadeService()
    service.run() # Runs forever
```

## Testing

Unit tests are provided ("make test").


## Debian Packaging

Debian packaging files are provided for internal BBC R&D use.
These packages depend on packages only available from BBC R&D internal mirrors, and will not build in other environments. For use outside the BBC please use python installation method.
