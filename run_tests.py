import json
import unittest
from handlers.queue import RedisQueue
from handlers.stack import RedisStack
from handlers.hashset import RedisHashSet
from handlers.key_value import RedisKeyValue
from handlers.list import RedisList
from handlers.set import RedisSet
from handlers.sorted_set import RedisSortedSet
from handlers.stream import RedisStream
from handlers.pubsub import RedisPubSub



class TestRedisQueue(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.queue = RedisQueue('test_queue')
        cls.first_data = {'name': 'Ozan Metin', 'email': 'example@example.com'}
        cls.last_data = {'name': 'John Doe', 'email': 'example@example.com'}
        cls.queue.clear()

    def test_enqueue_and_size(self):
        self.queue.set(self.first_data)
        self.queue.set(self.last_data)
        self.assertEqual(self.queue.size(), 2)

    def test_dequeue_first_in_first_out(self):
        self.queue.set(self.first_data)
        self.queue.set(self.last_data)
        expected_first_data = self.queue.get()
        self.assertEqual(expected_first_data, self.first_data)
        self.assertEqual(self.queue.size(), 1)

        expected_last_data = self.queue.get()
        self.assertEqual(expected_last_data, self.last_data)
        self.assertEqual(self.queue.size(), 0)

    def test_is_empty(self):
        self.queue.clear()
        self.assertTrue(self.queue.is_empty())

    def test_clear(self):
        self.queue.set(self.first_data)
        self.queue.set(self.last_data)
        self.assertEqual(self.queue.size(), 2)
        self.queue.clear()
        self.assertEqual(self.queue.size(), 0)

    @classmethod
    def tearDownClass(cls):
        cls.queue.clear()
        

class TestRedisStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.stack = RedisStack('test_stack')
        cls.first_data = {'name': 'Ozan Metin', 'email': 'example@example.com'}
        cls.last_data = {'name': 'John Doe', 'email': 'example@example.com'}
        cls.stack.clear()

    def test_push_and_size(self):
        self.stack.set(self.first_data)
        self.stack.set(self.last_data)
        self.assertEqual(self.stack.size(), 2)

    def test_pop_last_in_first_out(self):
        self.stack.set(self.first_data)
        self.stack.set(self.last_data)
        expected_last_data = self.stack.get()
        self.assertEqual(expected_last_data, self.last_data)
        self.assertEqual(self.stack.size(), 1)

        expected_first_data = self.stack.get()
        self.assertEqual(expected_first_data, self.first_data)
        self.assertEqual(self.stack.size(), 0)

    def test_is_empty(self):
        self.stack.clear()
        self.assertTrue(self.stack.is_empty())

    def test_clear(self):
        self.stack.set(self.first_data)
        self.stack.set(self.last_data)
        self.assertEqual(self.stack.size(), 2)
        self.stack.clear()
        self.assertEqual(self.stack.size(), 0)

    @classmethod
    def tearDownClass(cls):
        cls.stack.clear()


class TestRedisHashSetHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hash_set = RedisHashSet('test_hash_set')
        cls.test_key = 'test_key'
        cls.test_value = {'name': 'Test Name', 'email': 'test@example.com'}
        cls.hash_set.clear()

    def test_set_and_get(self):
        self.hash_set.set(self.test_key, self.test_value)
        retrieved_value = self.hash_set.get(self.test_key)
        self.assertEqual(retrieved_value, self.test_value)

    def test_get_nonexistent_key(self):
        retrieved_value = self.hash_set.get('nonexistent_key')
        self.assertIsNone(retrieved_value)

    def test_get_all(self):
        self.hash_set.set('key1', {'data': 'value1'})
        self.hash_set.set('key2', {'data': 'value2'})
        all_data = self.hash_set.get_all()
        expected_data = {
            'key1': {'data': 'value1'},
            'key2': {'data': 'value2'}
        }
        self.assertEqual(all_data, expected_data)

    def test_delete(self):
        self.hash_set.set(self.test_key, self.test_value)
        self.hash_set.delete(self.test_key)
        self.assertIsNone(self.hash_set.get(self.test_key))

    def test_exists(self):
        self.hash_set.set(self.test_key, self.test_value)
        self.assertTrue(self.hash_set.exists(self.test_key))
        self.hash_set.delete(self.test_key)
        self.assertFalse(self.hash_set.exists(self.test_key))

    def test_clear(self):
        self.hash_set.set(self.test_key, {'data': 'value'})
        self.assertTrue(self.hash_set.exists(self.test_key))
        self.hash_set.clear()
        self.assertFalse(self.hash_set.exists(self.test_key))

    @classmethod
    def tearDownClass(cls):
        cls.hash_set.clear()


class TestRedisKeyValueHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kv_store = RedisKeyValue('test_kv_store')
        cls.test_key = 'test_key'
        cls.test_value = {'name': 'Test Name', 'email': 'test@example.com'}
        cls.kv_store.clear()

    def test_set_and_get(self):
        self.kv_store.set(self.test_key, self.test_value)
        retrieved_value = self.kv_store.get(self.test_key)
        self.assertEqual(retrieved_value, self.test_value)

    def test_get_nonexistent_key(self):
        retrieved_value = self.kv_store.get('nonexistent_key')
        self.assertIsNone(retrieved_value)

    def test_get_all(self):
        self.kv_store.set('key1', {'data': 'value1'})
        self.kv_store.set('key2', {'data': 'value2'})
        all_data = self.kv_store.get_all()
        expected_data = {
            'key1': {'data': 'value1'},
            'key2': {'data': 'value2'}
        }
        self.assertEqual(all_data, expected_data)

    def test_delete(self):
        self.kv_store.set(self.test_key, self.test_value)
        self.kv_store.delete(self.test_key)
        self.assertIsNone(self.kv_store.get(self.test_key))

    def test_exists(self):
        self.kv_store.set(self.test_key, self.test_value)
        self.assertTrue(self.kv_store.exists(self.test_key))
        self.kv_store.delete(self.test_key)
        self.assertFalse(self.kv_store.exists(self.test_key))

    def test_clear(self):
        self.kv_store.set(self.test_key, {'data': 'value'})
        self.assertTrue(self.kv_store.exists(self.test_key))
        self.kv_store.clear()
        self.assertFalse(self.kv_store.exists(self.test_key))

    def test_set_with_ttl(self):
        self.kv_store.set(self.test_key, self.test_value, ttl=1)
        self.assertTrue(self.kv_store.exists(self.test_key))
        import time
        time.sleep(2)
        self.assertFalse(self.kv_store.exists(self.test_key))

    @classmethod
    def tearDownClass(cls):
        cls.kv_store.clear()


class TestRedisList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.list_instance = RedisList('test_list')

    def setUp(self):
        self.list_instance.clear()

    def test_set_and_get(self):
        value1 = {'name': 'Alice'}
        value2 = {'name': 'Bob'}

        self.list_instance.set(value1)
        self.list_instance.set(value2)

        self.assertEqual(self.list_instance.get(0), value1)
        self.assertEqual(self.list_instance.get(1), value2)
        self.assertEqual(self.list_instance.size(), 2)

    def test_delete(self):
        value = {'name': 'Alice'}

        self.list_instance.set(value)
        self.list_instance.delete(value)

        self.assertIsNone(self.list_instance.get(0))
        self.assertEqual(self.list_instance.size(), 0)


class TestRedisSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.set_instance = RedisSet('test_set')

    def setUp(self):
        self.set_instance.clear()

    def test_add_and_contains(self):
        value1 = {'name': 'Alice'}
        value2 = {'name': 'Bob'}

        self.set_instance.set(value1)
        self.assertTrue(self.set_instance.exists(value1))

        self.set_instance.set(value2)
        self.assertTrue(self.set_instance.exists(value2))

    def test_remove(self):
        value = {'name': 'Alice'}

        self.set_instance.set(value)
        self.set_instance.delete(value)

        self.assertFalse(self.set_instance.exists(value))
        self.assertEqual(self.set_instance.size(), 0)


class TestRedisSortedSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sorted_set_instance = RedisSortedSet('test_sorted_set')

    def setUp(self):
        self.sorted_set_instance.clear()

    def test_add_and_get_by_score(self):
        value1 = {'name': 'Alice'}
        value2 = {'name': 'Bob'}

        self.sorted_set_instance.set(value1, 1.0)
        self.assertEqual(self.sorted_set_instance.get(value1), 1.0)

        self.sorted_set_instance.set(value2, 2.0)
        self.assertEqual(self.sorted_set_instance.get(value2), 2.0)

    def test_remove(self):
        value = {'name': 'Alice'}
        self.sorted_set_instance.set(value, 1.0)
        self.sorted_set_instance.delete(value)
        self.assertIsNone(self.sorted_set_instance.get(value))


class TestRedisPubSub(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pubsub_instance = RedisPubSub()

    def setUp(self):
        pass  # No need to clear or reset anything for Pub/Sub

    def test_publish_and_subscribe(self):
        channel = 'test_channel'
        message = {'message': 'Hello'}

        received_message = {}

        def callback(msg):
            nonlocal received_message
            received_message = json.loads(msg['data'])

        self.pubsub_instance.subscribe(channel, callback)
        self.pubsub_instance.publish(channel, message)

        # Wait for message to be received
        self.pubsub_instance._conn.execute_command('DEBUG', 'SLEEP', '1')

        self.assertEqual(received_message, message)


if __name__ == '__main__':
    unittest.main()
