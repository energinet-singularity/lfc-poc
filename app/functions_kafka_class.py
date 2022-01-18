from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import dumps, loads
import sys
import os
from functions_lfc import add_to_log


class KafkaHelper:
    """
    Handles communication with Kafka in terms of both producing and consuming messages.

    Attributes
    ----------
    group id : str, optional
        kafka consumer group id used to keep track of consumed message offset (default is None)
    auto_offset_reser : str, optional
        kafka auto offset reset, which can be set to latest or earliest (default is earliest)
    enable_auto_commit : boolen, optional
        ???

    """
    # TODO: verify inputs (ie. is a list)
    def __init__(self,
                 group_id=None,
                 auto_offset_reset='earliest',
                 enable_auto_commit=False,
                 topics_consumed_list=[],
                 topics_produced_list=[],
                 poll_timeout_ms=100):
        # attributes set on object init
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.topics_consumed_list = topics_consumed_list
        self.topics_produced_list = topics_produced_list
        self.topic_list = list(set(self.topics_produced_list + self.topics_consumed_list))
        self.poll_timeout_ms = poll_timeout_ms

        # attributes
        # TODO split in init and uninit
        # init by methods on creation
        self.bootstrap_servers = None
        self.consumer = None
        self.producer = None
        self.unavbl_topics = []
        self.topic_partions_dict = {}
        self.begin_offset_topic_partitions_dict = {}
        self.end_offset_topic_partitions_dict = {}
        self.last_read_offset_topic_partitions_dict = {}

        # not init on creation
        self.topic_latest_message_timestamp_dict = {}
        self.topic_latest_message_value_dict = {}
        self.message_value = None

        # methods called on init
        self.set_kafka_brooker_from_env()
        # TODO only init if consumed and produced topics
        # TODO warning if no topics at all?
        self.init_consumer()
        if topics_consumed_list:
            self.subscribe_topics()
        if topics_produced_list:
            self.init_producer()
        # always
        self.list_unavbl_topics()
        self.init_topic_partitions()
        self.init_topic_partitions_dict()
        self.init_topic_partitions_begin_offsets_dict()
        self.init_topic_partitions_end_offsets_dict()
        self.init_topic_partitions_last_read_offset_dict()

    # Method:
    def set_kafka_brooker_from_env(self):
        self.bootstrap_servers = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")

    # Method: Initializes Kafka comsumer, consuming from beginning af partitions.
    def init_consumer(self):
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                          group_id=self.group_id,
                                          value_deserializer=lambda x: loads(x.decode('utf-8')),
                                          auto_offset_reset=self.auto_offset_reset,
                                          enable_auto_commit=self.enable_auto_commit)
            add_to_log("Info: Kafka consumer connection established.")
        except Exception as e:
            add_to_log(f"Error: Kafka Consumer connection failed with message: '{e}'.")
            sys.exit(1)

    # Method: Initializes Kafka producer.
    def init_producer(self):
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                          value_serializer=lambda x: dumps(x).encode('utf-8'))
            add_to_log("Info: Kafka producer connection established.")
        except Exception as e:
            add_to_log(f"Error: Kafka producer connection failed with message: '{e}'.")
            sys.exit(1)

    # Method: Subscribes consumer to topics
    def subscribe_topics(self):
        try:
            self.consumer.subscribe(self.topics_consumed_list)
            add_to_log(f"Info: Kafka consumer subscribed to topics: '{self.topics_consumed_list}'.")
        except Exception as e:
            add_to_log(f"Error: Kafka consumer subscribtion to topics: '{self.topics_consumed_list}' " +
                       f"failed with message: '{e}'.")
            sys.exit(1)

    # Method: Dummy Poll, which is needed to force assignment of partitions after subsribtion to topics.
    def init_topic_partitions(self):
        # TODO check if using a dummy poll is the correct way
        # TODO handle that when making dummy poll, if auto comit is enabled, then the offset will shift unessecary
        try:
            self.consumer.poll(timeout_ms=self.poll_timeout_ms)
            add_to_log("Info: Initial poll done. TopicPartitions are now assigned.")
        except Exception as e:
            add_to_log(f"Error: Initial poll failed with message '{e}'.")
            sys.exit(1)

    # Method: Verify if topics exist in Kafka Brooker.
    # TODO rename
    def list_unavbl_topics(self):
        self.unavbl_topics = []
        for topic in self.topic_list:
            try:
                if topic not in self.consumer.topics():
                    self.unavbl_topics.append(topic)
            except Exception as e:
                add_to_log(f"Error: Verifying if topic: '{topic}' exists failed with message: '{e}'.")
                sys.exit(1)

        if self.unavbl_topics:
            add_to_log(f"Error: The Topic(s): {self.unavbl_topics} does not exist.")
            sys.exit(1)

    # Method:
    # TODO rename?
    # TODO dict must be inint first? (report if not init)
    def list_empty_topics(self):
        empty_topics = []
        for topic in self.topic_latest_message_value_dict:
            if self.topic_latest_message_value_dict[topic] is None:
                empty_topics.append(topic)
            if empty_topics:
                add_to_log(f"Warning: No data was availiable on consumed Kafka Topic(s): {empty_topics}.")

    # Method Create a dictionary with partions avlaiable for each topic.
    def init_topic_partitions_dict(self):
        if type(self.topic_list) != list:
            add_to_log(f"Error: Supplied topics must have the type 'list' but is {type(self.topic_list)}")
            sys.exit(1)

        self.topic_partitions_dict = {}
        try:
            for topic in self.topic_list:
                self.topic_partitions_dict[topic] = [TopicPartition(topic, p)
                                                     for p in self.consumer.partitions_for_topic(topic)]
        except Exception as e:
            add_to_log(f"Error: Making dictionary of lists for topic partitions for topic: '{topic}' " +
                       f"failed with message: '{e}'.")
            sys.exit(1)

    # Method: Create a dictionary with topic partion --> begin offsets
    def init_topic_partitions_begin_offsets_dict(self):
        self.begin_offset_topic_partitions_dict = {}

        try:
            for topic in self.topic_list:
                self.begin_offset_topic_partitions_dict[topic] = self.consumer.beginning_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
        except Exception as e:
            add_to_log(f"Error: Getting latest offset for partitions for topic: '{topic}' failed with message '{e}'.")
            sys.exit(1)

    # Method: Create a dictionary with topic partion --> end offsets
    def init_topic_partitions_end_offsets_dict(self):
        self.end_offset_topic_partitions_dict = {}
        try:
            for topic in self.topic_list:
                self.end_offset_topic_partitions_dict[topic] = self.consumer.end_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
        except Exception as e:
            add_to_log(f"Error: Getting end offset for partitions for topic: '{topic}' failed with message: '{e}'.")
            sys.exit(1)

    # Method:
    def init_topic_partitions_last_read_offset_dict(self):

        # TODO calling init again?
        self.init_topic_partitions_begin_offsets_dict()

        self.last_read_offset_topic_partition = self.begin_offset_topic_partitions_dict

        for topic in self.topic_list:
            for topic_partition in self.topic_partitions_dict[topic]:
                begin_offset = self.last_read_offset_topic_partition[topic][topic_partition]
                if begin_offset != 0:
                    self.last_read_offset_topic_partition[topic][topic_partition] = begin_offset-1

    # Method:
    def init_topic_latest_msg_dicts(self):
        self.topic_latest_message_timestamp_dict = {}
        self.topic_latest_message_value_dict = {}
        for topic in self.topics_consumed_list:
            self.topic_latest_message_timestamp_dict[topic] = 0
            self.topic_latest_message_value_dict[topic] = None

    # Method: Seek partitions to latest availiable message
    def seek_topic_partitions_latest(self):
        # TODO calling init again?
        self.init_topic_partitions_end_offsets_dict()

        try:
            # loop topics and seek all partitions to latest availiable message
            for topic in self.topics_consumed_list:
                # loop all partitions for topic
                for topic_partion in self.topic_partitions_dict[topic]:
                    # if they have messages, per partition
                    if self.end_offset_topic_partitions_dict[topic][topic_partion] > 0:
                        # seek to highest offset
                        partition = self.topic_partitions_dict[topic][topic_partion.partition]
                        offset = self.end_offset_topic_partitions_dict[topic][topic_partion]-1
                        self.consumer.seek(partition, offset)
        except Exception as e:
            add_to_log(f"Error: Seeking consumer: '{partition}' to offset: {offset} failed with message '{e}'.")
            sys.exit(1)

    # Method: Get latest message value per topic and return it in dictionary - using .poll
    def get_latest_topic_messages_to_dict_poll_based(self):
        # TODO modify this to include timeout for max time spend on polls?

        # init dictionary with Topic -> TopicPartitions
        # TODO reinint every time?
        self.init_topic_partitions_dict()

        # init dictionary with Topic,TopicPartition -> begin_offset-1 unless 0 (used for tracking last read offset)
        # TODO verify if this works for empty topic
        self.init_topic_partitions_last_read_offset_dict()

        # dictionaries for holding latest timestamp and value for each consumed topic
        self.init_topic_latest_msg_dicts()

        # Seek all partitions for consumed topics to latest availiable message
        self.seek_topic_partitions_latest()

        # init dictionary with Topic,TopicPartition -> end_offset
        self.init_topic_partitions_end_offsets_dict()

        # poll data
        is_polling = True
        while is_polling:

            data_object = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=None)

            if data_object:

                # loop all messages returned by poll per partition
                for topic_partition in data_object:
                    # loop all messages for partition
                    for msg in range(0, len(data_object[topic_partition])):
                        # if timestamp is latest then update values in dict
                        topic = data_object[topic_partition][msg].topic
                        if data_object[topic_partition][msg].timestamp > self.topic_latest_message_timestamp_dict[topic]:
                            self.topic_latest_message_timestamp_dict[topic] = data_object[topic_partition][msg].timestamp
                            self.topic_latest_message_value_dict[topic] = data_object[topic_partition][msg].value
                        # update last read mesage offset dict
                        self.last_read_offset_topic_partition[topic][topic_partition] = data_object[topic_partition][msg].offset

            # Make list of partitions for which last message offset has not yet been reached
            # TODO function?
            topic_partitions_not_reached_last_offset = []
            for topic in self.topics_consumed_list:
                for topic_partition in self.topic_partitions_dict[topic]:
                    if self.last_read_offset_topic_partition[topic][topic_partition] < self.end_offset_topic_partitions_dict[topic][topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partions have been consumed till latest offset, break out of conusmer loop
            count_part = len(topic_partitions_not_reached_last_offset)
            if count_part == 0:
                is_polling = False

    # method:
    def get_msg_val_from_dict(self, tp_nm, msg_val_nm, default_val=None, precision=3):
        # TODO simplify/make smarter (Avro schema?)
        # TODO error handling if message value name not found
        # TODO add try/catch
        if self.topic_latest_message_value_dict[tp_nm] is None:
            add_to_log(f"Warning: Value: {msg_val_nm} is not avialiable from topic: " +
                       f"'{tp_nm}'. Setting to default value: '{default_val}'.")
            self.message_value = default_val
        else:
            # TODO build safety again wrongly formattet message
            self.message_value = self.topic_latest_message_value_dict[tp_nm][msg_val_nm]

        if type(self.message_value) == float:
            self.message_value = round(self.message_value, precision)

    def produce_message(self, topic_name, msg_value):
        # TODO verify if topic name is in producer list?
        try:
            self.producer.send(topic_name, value=msg_value)
            return True
        except Exception as e:
            add_to_log(f"Error: Sending message to Kafka failed with message: '{e}'.")
            sys.exit(1)


def get_msg_val_from_dict(msg_val_dict: dict, tp_nm: str, msg_val_nm: str, default_val=None, precision=3):
    """
    """
    # TODO simplify/make smarter (Avro schema?)
    # TODO error handling if message value name not found
    # TODO add try/catch
    if msg_val_dict[tp_nm] is None:
        add_to_log(f"Warning: Value: {msg_val_nm} is not avialiable from topic: " +
                   f"'{tp_nm}'. Setting to default value: '{default_val}'.")
        msg_val = default_val
    else:
        # TODO build safety again wrongly formattet message
        msg_val = msg_val_dict[tp_nm][msg_val_nm]

    if type(msg_val) == float:
        msg_val = round(msg_val, precision)

    return msg_val


"""
# TODO: add this funtion to class
def get_latest_topic_messages_to_dict_loop_based(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    # Get latest message value per topic and return it in dictionary - using consumer loop

    # Latest message is determined on timestamp. Will loop all partitions.

    # TODO put everything in a try catch?
    # TODO modify this to include timeout logic?

    # init dictionary with Topic -> TopicPartitions
    topic_partitions_dict = create_topic_partitions_dict(consumer=consumer, topic_list=topic_list)

    # init dictionary with Topic,TopicPartition -> begin_offset-1 unless 0 (used for tracking last read offset)
    # TODO verify if this works for empty topic
    last_read_offset_topic_partition = create_topic_partitions_last_read_offset_dict(consumer=consumer, topic_list=topic_list)

    # dictionaries for holding latest timestamp and value for each consumed topic
    topic_latest_message_timestamp_dict, topic_latest_message_value_dict = init_topic_latest_msg_dicts(topic_list=topic_list)

    # Seek all partitions for consumed topics to latest availiable message
    seek_topic_partitions_latest(consumer=consumer, topic_list=topic_list)

    # init dictionary with Topic,TopicPartition -> end_offset
    last_offset_dict = create_topic_partitions_end_offsets_dict(consumer=consumer, topic_list=topic_list)

    for message in consumer:

        # if timestamp of read message is newer that last read message from topic, update dict
        if message.timestamp > topic_latest_message_timestamp_dict[message.topic]:
            topic_latest_message_timestamp_dict[message.topic] = message.timestamp
            topic_latest_message_value_dict[message.topic] = message.value

        # update last read mesage offset
        last_read_offset_topic_partition[message.topic][TopicPartition(message.topic, message.partition)] = message.offset

        # Make list of partitions for which last message offset has not yet been reached
        # TODO make function
        topic_partitions_not_reached_last_offset = []
        for topic in topic_list:
            for topic_partition in topic_partitions_dict[topic]:
                if last_read_offset_topic_partition[topic][topic_partition] < last_offset_dict[topic][topic_partition]-1:
                    topic_partitions_not_reached_last_offset.append(topic_partition)

        # If all partions have been consumed till latest offset, break out of conusmer loop
        count_part = len(topic_partitions_not_reached_last_offset)
        if count_part == 0:
            break

    return topic_latest_message_value_dict

"""
