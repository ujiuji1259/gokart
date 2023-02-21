import bz2
import json
from logging import getLogger

import luigi
from luigi import task_register

import gokart

logger = getLogger(__name__)


class TaskInstanceParameter(luigi.Parameter):

    def __init__(self, bound=gokart.TaskOnKart, *args, **kwargs):
        if isinstance(bound, type):
            self._bound = bound
        else:
            raise ValueError(f'bound must be a type, not {type(bound)}')
        super().__init__(*args, **kwargs)

    @staticmethod
    def _recursive(param_dict):
        params = param_dict['params']
        task_cls = task_register.Register.get_task_cls(param_dict['type'])
        for key, value in task_cls.get_params():
            if key in params:
                params[key] = value.parse(params[key])
        return task_cls(**params)

    @staticmethod
    def _recursive_decompress(s):
        s = dict(luigi.DictParameter().parse(s))
        if 'params' in s:
            s['params'] = TaskInstanceParameter._recursive_decompress(bz2.decompress(bytes.fromhex(s['params'])).decode())
        return s

    def parse(self, s):
        if isinstance(s, str):
            s = self._recursive_decompress(s)
        return self._recursive(s)

    def serialize(self, x):
        params = bz2.compress(json.dumps(x.to_str_params(only_significant=True)).encode()).hex()
        values = dict(type=x.get_task_family(), params=params)
        return luigi.DictParameter().serialize(values)

    def normalize(self, v):
        if not isinstance(v, self._bound):
            raise ValueError(f'{v} is not an instance of {self._bound}')
        return v


class _TaskInstanceEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, luigi.Task):
            return TaskInstanceParameter().serialize(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ListTaskInstanceParameter(luigi.Parameter):

    def __init__(self, bound=gokart.TaskOnKart, *args, **kwargs):
        if isinstance(bound, type):
            self._bound = bound
        else:
            raise ValueError(f'bound must be a type, not {type(bound)}')
        super().__init__(*args, **kwargs)

    def parse(self, s):
        return [TaskInstanceParameter().parse(x) for x in list(json.loads(s))]

    def serialize(self, x):
        return json.dumps(x, cls=_TaskInstanceEncoder)

    def normalize(self, values):
        for v in values:
            if not isinstance(v, self._bound):
                raise ValueError(f'{v} is not an instance of {self._bound}')
        return values


class ExplicitBoolParameter(luigi.BoolParameter):

    def __init__(self, *args, **kwargs):
        luigi.Parameter.__init__(self, *args, **kwargs)

    def _parser_kwargs(self, *args, **kwargs):
        return luigi.Parameter._parser_kwargs(*args, *kwargs)
