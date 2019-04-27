import baker
import sys

from .producers import PersistentAsyncProducer, PersistentProducer, VolitileProducer
from .data import SAMPLE_PAYLOAD

PRODUCERS = {
    'persistent_async': PersistentAsyncProducer,
    'persistent_sync': PersistentProducer,
    'volatile': VolitileProducer
}


HEADER_FORMAT = '%10s %20s\n'
ITEM_FORMAT   = '%10d %20d\n'
FOOTER_FORMAT = '%10s %20d\n'


@baker.command
def benchmark(producer, topic, times=100, key_prefix='test_'):
    if producer not in PRODUCERS:
        sys.stderr.write("Producer must be one of: %s\n" % "|".join(PRODUCERS.keys()))
        sys.exit(1)

    kafka_producer = PRODUCERS[producer]()
    sys.stdout.write(HEADER_FORMAT % ('#', producer))

    total_time = 0

    for n in range(1, times + 1):
        key = key_prefix + str(n)
        value = 'Message ' + str(n) + '\n' + SAMPLE_PAYLOAD
        ms = kafka_producer.produce(topic, value, key)
        total_time += ms
        sys.stdout.write(ITEM_FORMAT % (n, ms))

    done_ms = kafka_producer.done()
    total_time += done_ms
    sys.stdout.write(FOOTER_FORMAT % ('done()', done_ms))
    sys.stdout.write(FOOTER_FORMAT % ('Total', total_time))
    sys.stdout.write(FOOTER_FORMAT % ('Avg', total_time/times))

baker.run()
