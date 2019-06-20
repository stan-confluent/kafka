import time

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int


class BasicTest(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(BasicTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = 'test_topic'
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
            "partitions": 3,
            "replication-factor": 3,
            'configs': {"min.insync.replicas": 2}}})
        self.zk.start()

    def create_producer_and_consumer(self):
        self.producer = VerifiableProducer(
            self.test_context, 1, self.kafka, self.topic, throughput=100)

        self.consumer = ConsoleConsumer(
            self.test_context, 1, self.kafka, self.topic, consumer_timeout_ms=60000, message_validator=is_int)

    # @cluster(num_nodes=9)
    # def super_basic_test(self):
    #     self.kafka.start()

    @cluster(num_nodes=9)
    def basic_test(self):
        self.kafka.reset_listeners()
        self.kafka.add_client_listener(security_protocol=SecurityConfig.SASL_SSL)
        self.kafka.add_interbroker_listener(name='CUSTOM', security_protocol=SecurityConfig.SASL_SSL)
        self.kafka.client_sasl_mechanism = SecurityConfig.SASL_MECHANISM_GSSAPI
        self.kafka.interbroker_sasl_mechanism = SecurityConfig.SASL_MECHANISM_PLAIN

        self.kafka.start()

        # self.create_producer_and_consumer()
        #
        # self.run_produce_consume_validate(lambda: time.sleep(1))
