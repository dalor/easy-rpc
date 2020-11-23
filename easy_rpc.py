from typing import Any
import json
import asyncio
from aio_pika.patterns import RPC
from aio_pika import connect_robust
from pydantic import BaseModel, ValidationError

class RPCConfig():
    """
    Class representing config for RabbitMQ connection

    Attributes
    ----------
    config : dict
        Dictionary representing config values
    """

    def __init__(self, path: str = None):
        """
        Parameters
        ----------
        path : str
            String path to json config file (default: None)
        """

        with open(path, "r") as file:
            self.config = json.load(file)

    def __getitem__(self, item: Any):
        return self.config.get(item)

class JsonRPC(RPC):
    """
    Expanded class that implements json serialization and deserialization.
    """

    SERIALIZER = json
    CONTENT_TYPE = "application/json"

    def serialize(self, data: Any) -> bytes:
        """
        Serialize data

        Parameters
        ----------
        data : Any
            Object to serialize

        Returns
        -------
        bytes
            json serialized object
        """

        return self.SERIALIZER.dumps(
            data, ensure_ascii=False, default=repr
            ).encode('utf-8')

    def serialize_exception(self, exception: Exception) -> bytes:
        """
        Serialize raised exception

        Parameters
        ----------
        exception : Any
            Exception object to serialize

        Returns
        -------
        bytes
            serialized object into json
        """

        return self.serialize(
            {
                "error": {
                    "type": exception.__class__.__name__,
                    "message": repr(exception),
                    "args": exception.args,
                }
            }
        )

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize data.

        Parameters
        ----------
        data : bytes
            Deserializable object

        Returns
        -------
        Any
            deserialized object
        """

        return self.SERIALIZER.loads(data.decode('utf-8'))


class EasyRpcException(Exception):

    def __init__(self, error):
        self.error = error

    def __repr__(self):
        return 'EasyRpcException({})'.format(self.error)

def rpc_model_validator(func):

    output_model = func.__annotations__.get('return')

    assert(output_model and output_model.__class__ == BaseModel.__class__)

    annotations = {key: value for key, value in func.__annotations__.items(
          ) if key != 'return' and value.__class__ == BaseModel.__class__}

    assert(annotations)

    async def rpc_validator_func(**data):

        try:
            return {'ok': True, 'data': (await func(**{key: value(**data) for key, value in annotations.items()})).dict(), 'error': None}
        except ValidationError as err:
            return {'ok': False, 'data': None, 'error': err.errors()}
        except Exception as err:
            return {'ok': False, 'data': None, 'error': str(err)}

    rpc_validator_func.__name__ = func.__name__

    return rpc_validator_func


class RPCRouter():
    """
    Class represents an RPC routes(callbacks) for the app.
    """

    def __init__(self):
        self.procedures = []

    def procedure(self, method_name: str = None, **kwargs):
        """
        Decorator to register new remote procedure for this router.

        Parameters
        ----------
        method_name : str
            Name of the method (default: function name)
        **kwargs : dict
            Keyword arguments to pass into **register** function
        """

        def decorator(func):
            kwargs.update({
                "method_name": method_name or func.__name__,
                "func": rpc_model_validator(func)
            })
            self.procedures.append(kwargs)
            return func

        return decorator


class EasyRPC():
    """
    Class represents an RPC template application for RabbitMQ
    """

    def __init__(self, config: RPCConfig = None, loop = None):
        """
        Parameters
        ----------
        config : RPCConfig
            Configuration class for RabbitMQ connection
        """

        self.procedures = []

        self.__config = config
        self.__loop = loop or asyncio.get_event_loop()
        self.__connection = None 
        self.__rpc = None

    async def __create_connection(self, config: RPCConfig):
        connection = await connect_robust(**config.config)

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=4)
        rpc = await JsonRPC.create(channel)

        return connection, rpc

    def procedure(self, method_name: str = None, **kwargs):
        """
        Decorator to register new remote procedure for this app.

        Parameters
        ----------
        method_name : str
            Name of the method (default: function name)
        **kwargs : dict
            Keyword arguments to pass into **register** function
        """

        def decorator(func):
            kwargs.update({
                "method_name": method_name or func.__name__,
                "func": rpc_model_validator(func)
            })
            self.procedures.append(kwargs)
            return func

        return decorator

    def add_router(self, router: RPCRouter):
        """
        Add router with registered routes.

        Parameters
        ----------
        router : RPCRouter
            Router class with registered routes.
        """

        self.procedures.extend(router.procedures)

    async def run_procedure(self, method_name: str, args: dict, **kwargs):
        """
        Method to run remote procedure.

        Parameters
        ----------
        method_name : str
            Name of the method to run. Must match remote registered method name
        args : dict
            Arguments to pass into method
        **kwargs : dict
            Additional arguments to pass into **call** function.
        """

        res = await self.__rpc.call(method_name, kwargs=args, **kwargs)

        if res.get('ok'):

            return res.get('data')

        else:
            raise EasyRpcException(res.get('error'))

    def run_forever(self, loop=None):
        loop = loop or self.__loop
        loop.call_soon(self.run)
        loop.run_forever()
 

    def run(self):
        """
        Run RPC server with all procedures
        """ 
        async def run_():

            self.__connection, self.__rpc = await self.__create_connection(self.__config)

            for procedure in self.procedures:
                await self.__rpc.register(**procedure)              

        return asyncio.ensure_future(run_())

    def stop(self):
        """
        Close connection to RabbitMQ.
        """

        self.__loop.run_until_complete(self.__connection.close())
