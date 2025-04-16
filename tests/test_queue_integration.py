from multiprocessing import Process, Manager


def producer(q, count):
    for i in range(count):
        q.put(i)


def consumer(q, count, results):
    for _ in range(count):
        item = q.get()
        results.append(item)


def test_multiple_processes():
    count = 100
    with Manager() as manager:
        results = manager.list()
        q = manager.Queue()
        p1 = Process(target=producer, args=(q, count))
        p2 = Process(target=consumer, args=(q, count, results))
        p1.start()
        p2.start()
        p1.join()
        p2.join()
        assert len(results) == count
