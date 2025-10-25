import inspect
from airflow.providers.common.compat.standard.operators import PythonOperator
from airflow.sdk.bases.decorator import task_decorator_factory
from typing import Callable, Sequence , Any
from airflow.sdk.definitions.context import Context
from airflow.sdk.bases.decorator import TaskDecorator
import warnings

# TODO: update keys
BALLISTA_CONTEXT_KEYS = ["ctx"]

class BallistaDecoratedOperator(PythonOperator):
    custom_operator_name = "@task.ballista"

    def __init__(
            self,
            python_callable : Callable,
            op_args: Sequence | None = None,
            op_kwargs: dict | None = None,
            conn_id : str | None = None,
            config_kwargs: dict | None = None,
            **kwargs,
    ):
        self.conn_id = conn_id
        self.config_kwargs = config_kwargs or {}
        signature = inspect.signature(python_callable)

        parameters = [
            param.replace(default=None) if param.name in BALLISTA_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]

        python_callable.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]

        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }

        super().__init__(
            #kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    def execute(self, context: Context):
        from ..ballista import BallistaBuilder

        ballista = BallistaBuilder()\
            .config("ballista.job.name", f"{self.dag_id}-{self.task_id}")

        if self.conn_id is None:
            ctx = ballista.standalone()
        else:
            raise NotImplementedError("Connection handling is not implemented yet.")
            # we handle both spark connect and spark standalone
            # conn = BallistaHook.get_connection(self.conn_id)
            # for key, value in conn.extra_dejson.items():
            #     ballista = ballista.config(key, value)
            # if conn.port:
            #     ctx = ballista.remote(f"df://{conn.host}:{conn.port}")
            # elif conn.host:
            #     ctx = ballista.remote(f"df://{conn.host}")
            # else:
            #     warnings.warn(
            #         f"The connection {self.conn_id} does not have a valid host or port. "
            #         "Falling back to standalone mode."
            #     )
            #     ctx = ballista.standalone()

        if not self.op_kwargs:
            self.op_kwargs = {}

        op_kwargs: dict[str, Any] = dict(self.op_kwargs)
        op_kwargs["ctx"] = ctx    
        self.op_kwargs = op_kwargs
        return super().execute(context)

def ballista_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=BallistaDecoratedOperator,
        **kwargs,
    )