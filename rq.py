import multiprocessing
import time
import logging
import json
import importlib
from types import FunctionType

from redis import Redis

_redis_url = None


def set_redis_url(url: str):
    global _redis_url
    _redis_url = url


'''
## 设计

### redis 中保存 3 类数据：

1. 全局计数器，为每个任务分配一个 id，key 的格式：[queue]:counter
2. 任务相关参数，值类型为 json 字符串，包含 module、 name、a、kw 几个字段；key 的格式：[queue]:task:[id]；过期时间 7 天
3. 任务队列，key 的格式：[queue]:tq，值类型为 list，任务进队列 LPUSH，任务出队列 RPOP
'''

class AdditionalEnDecoder(object):

    @classmethod
    def _serialize_func(cls, func):
        return {
                '__type__': 'function',
                '__module__': func.__module__,
                '__name__': func.__name__,
            }

    @classmethod
    def encode(cls, obj):
        if isinstance(obj, FunctionType):
            return cls._serialize_func(obj)
        return obj

    @classmethod
    def _deserialize_func(cls, meta):
        module = importlib.import_module(meta['__module__'])
        name = meta['__name__']
        if hasattr(module, name):
            return getattr(module, name)
        raise Exception('Can not find function {}.{}'.format(meta['__module__'], name))

    @classmethod
    def decode(cls, obj):
        if isinstance(obj, dict) and '__type__' in obj and '__module__' in obj and '__name__' in obj:
            if obj['__type__'] == 'function':
                return cls._deserialize_func(obj)
        return obj


class AsyncTask(object):

    expire_seconds = 7 * 24 * 3600

    def __init__(self, queue, func, redis_url):
        self._queue = queue
        self._counter_key = '{}:counter'.format(self._queue)
        self._task_queue_key = '{}:tq'.format(self._queue)
        self._func_module = func.__module__
        self._func_name = func.__name__

        self._redis_url = redis_url if redis_url is not None else _redis_url
        self._redis = None if self._redis_url is None else Redis.from_url(self._redis_url)

    def send(self, *a, **kw):
        if self._redis is None:
            self._redis_url = _redis_url
            self._redis = Redis.from_url(self._redis_url)

        task_id = self._redis.incr(self._counter_key)
        task_key = '{}:task:{}'.format(self._queue, task_id)

        # WARNING: 由于 json 模块只支持 dict, list, tuple, str, int, float, True, False, None 的序列化反序列化，
        # 所以默认函数的参数不能支持此外的类型
        # 目前实现了自定义简单的函数参数序列化反序列化，不过还是慎用复杂的函数参数
        task_info = json.dumps({
            'module': self._func_module,
            'name': self._func_name,
            'a': [AdditionalEnDecoder.encode(item) for item in a],
            'kw': dict([(k, AdditionalEnDecoder.encode(v)) for k, v in kw.items()])
        })
        self._redis.setex(task_key, AsyncTask.expire_seconds, task_info)
        self._redis.lpush(self._task_queue_key, task_key)


def async_task(queue: str = 'tq', redis_url: str = None):

    def decorator(f):
        at = AsyncTask(queue, f, redis_url)
        return at

    return decorator


class RQ(object):

    def __init__(self, queue: str = 'tq', redis_url: str = None):
        self._queue = queue
        self._task_queue_key = "{}:tq".format(queue)
        self._redis_url = redis_url if redis_url is not None else _redis_url
        self._redis = None

    def _recover(self):
        '''
        异常恢复逻辑
        '''
        keys = self._redis.keys('{}:task:*'.format(self._queue))
        if len(keys) == 0:
            return
        keys = set(keys)

        tq_length = self._redis.llen(self._task_queue_key)
        keys_in_queue = self._redis.lrange(0, tq_length-1) if tq_length > 0 else set()

        diff_keys = keys.difference(keys_in_queue)
        if len(diff_keys) == 0:
            return
        diff_keys = list(diff_keys)
        diff_keys.sort(reverse=True)
        logging.info("Recovering tasks={}".format(diff_keys))
        self._redis.rpush(self._task_queue_key, *diff_keys)

    def _runner(self):
        task_key = self._redis.rpop(self._task_queue_key)
        if task_key is None:
            time.sleep(1)
            return
        task_info = self._redis.get(task_key)
        if task_info is None:
            logging.info('The task_info of %s is None', task_key)
            return
        logging.info("task_key=%s, task_info=%s", task_key, task_info)
        task_info: dict = json.loads(task_info)

        module = importlib.import_module(task_info['module'])
        func_name = task_info['name']
        a = task_info['a']
        kw = task_info['kw']
        a = [AdditionalEnDecoder.decode(item) for item in a]
        kw = dict([(k, AdditionalEnDecoder.decode(v)) for k, v in kw.items()])
        if hasattr(module, func_name):
            getattr(module, func_name)(*a, **kw)
        else:
            logging.warning("Module '%s' does not have function '%s'", task_info['module'], func_name)
        self._redis.delete(task_key)

    def runner(self):
        while True:
            try:
                self._runner()
            except Exception as e:
                logging.error(e)

    def start(self):
        self._redis = Redis.from_url(self._redis_url)
        self._recover()
        p = multiprocessing.Process(target=self.runner)
        p.start()


if __name__ == '__main__':
    FORMAT = '%(asctime)-15s - %(levelname)s - %(pathname)s - %(lineno)d - %(process)d - %(threadName)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    set_redis_url('redis://127.0.0.1:6379/8')

    def callback(name: str):
        logging.info("{} in callback".format(name))
    def hello(name: str, callback, **kw):
        logging.info("Hello {}, kw={}".format(name, kw))
        callback(name)

    rq = RQ('test_queue', _redis_url)
    rq.start()
    logging.info('rq started!')
    hello = async_task("test_queue")(hello)
    hello.send('xiayf', callback)
    logging.info('hello.send(\'xiayf\')')
    hello.send('zhengqing', callback)
    logging.info('hello.send(\'zhengqing\')')
    hello.send('world', callback, a=1, cb=callback)
    logging.info('hello.send(\'world\', a=1)')
