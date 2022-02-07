from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import dumps, loads
import sys
import os
from functions_lfc import add_to_log


# TODO documentation
# TODO make error handling of value name not found, for methods which get message (avro?)
# TODO will not work if group id are used by multiple, how to check?
# TODO verify inputs (ie. is a list)
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
    def __init__(self,
                 group_id=None,
                 auto_offset_reset='earliest',
                 enable_auto_commit=False,
                 topics_consumed_list=[],
                 topics_produced_list=[],
                 poll_timeout_ms=100):

        # attributes set on object instansiation, either bt default or supplied values.
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.topics_consumed_list = topics_consumed_list
        self.topics_produced_list = topics_produced_list
        self.topic_list = list(set(self.topics_produced_list + self.topics_consumed_list))
        self.poll_timeout_ms = poll_timeout_ms

        # attributes init by methods on creation
        self.bootstrap_servers = None
        self.consumer = None
        self.producer = None

        # methods called to init attributes
        self.set_kafka_brooker_from_env()
        self.init_consumer()
        if topics_consumed_list:
            self.subscribe_topics()
        if topics_produced_list:
            self.init_producer()
        self.verify_topic_existence()
        self.init_topic_partitions()

    # Method: set kafka brooker from ENV or default to value
    def set_kafka_brooker_from_env(self):
        self.bootstrap_servers = os.environ.get('KAFKA_HOST', "my-cluster-kafka-bootstrap.kafka")

    # Method: Initializes Kafka comsumer.
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

    # Method: Dummy Poll, which is needed to force assignment of partitions after subsribtion to topics.
    def init_topic_partitions(self):
        # TODO check if using a dummy poll is the correct way
        # TODO handle that when making dummy poll, if auto comit is enabled, then the offset will shift unessecary?
        try:
            self.consumer.poll(timeout_ms=self.poll_timeout_ms)
            add_to_log("Info: Initial poll done. TopicPartitions are now assigned.")
        except Exception as e:
            add_to_log(f"Error: Initial poll failed with message '{e}'.")
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

    # Method: Verify if topics exist in Kafka Brooker.
    def verify_topic_existence(self):
        unavbl_topics = []
        for topic in self.topic_list:
            try:
                if topic not in self.consumer.topics():
                    unavbl_topics.append(topic)
            except Exception as e:
                add_to_log(f"Error: Verifying if topic: '{topic}' exists failed with message: '{e}'.")
                sys.exit(1)

        if unavbl_topics:
            add_to_log(f"Error: The Topic(s): {unavbl_topics} does not exist.")
            sys.exit(1)

    # Method: Create a dictionary with partions availiable for each topic.
    def create_topic_partitions_dict(self):
        if type(self.topic_list) != list:
            add_to_log(f"Error: Supplied topics must have the type 'list' but is {type(self.topic_list)}")
            sys.exit(1)

        topic_partitions_dict = {}
        try:
            for topic in self.topic_list:
                topic_partitions_dict[topic] = [TopicPartition(topic, p)
                                                for p in self.consumer.partitions_for_topic(topic)]
        except Exception as e:
            add_to_log(f"Error: Making dictionary of lists for topic partitions for topic: '{topic}' " +
                       f"failed with message: '{e}'.")
            sys.exit(1)

        return topic_partitions_dict

    # Method: Create a dictionary with topic partion --> begin offsets
    def create_topic_partitions_begin_offsets_dict(self):
        begin_offset_topic_partitions_dict = {}

        try:
            for topic in self.topic_list:
                begin_offset_topic_partitions_dict[topic] = self.consumer.beginning_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
        except Exception as e:
            add_to_log(f"Error: Getting latest offset for partitions for topic: '{topic}' failed with message '{e}'.")
            sys.exit(1)

        return begin_offset_topic_partitions_dict

    # Method: Create a dictionary with topic partion --> end offsets
    def create_topic_partitions_end_offsets_dict(self):
        end_offset_topic_partitions_dict = {}
        try:
            for topic in self.topic_list:
                end_offset_topic_partitions_dict[topic] = self.consumer.end_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
        except Exception as e:
            add_to_log(f"Error: Getting end offset for partitions for topic: '{topic}' failed with message: '{e}'.")
            sys.exit(1)

        return end_offset_topic_partitions_dict

    # Method: Create a dictionary with topic partion --> last read offset
    def create_topic_partitions_last_read_offset_dict(self):
        # TODO verify if this works for empty topic (works with minus 1?)

        topic_partitions_dict = self.create_topic_partitions_dict()
        last_read_offset_topic_partitions_dict = self.create_topic_partitions_begin_offsets_dict()

        for topic in self.topic_list:
            for topic_partition in topic_partitions_dict[topic]:
                begin_offset = last_read_offset_topic_partitions_dict[topic][topic_partition]
                if begin_offset != 0:
                    last_read_offset_topic_partitions_dict[topic][topic_partition] = begin_offset-1
                elif begin_offset == 0:
                    last_read_offset_topic_partitions_dict[topic][topic_partition] = -1

        return last_read_offset_topic_partitions_dict

    # TODO doc Method:
    def create_topic_latest_dicts(self):
        topic_latest_message_timestamp_dict = {}
        topic_latest_message_value_dict = {}
        for topic in self.topics_consumed_list:
            topic_latest_message_timestamp_dict[topic] = 0
            topic_latest_message_value_dict[topic] = None
        return topic_latest_message_timestamp_dict, topic_latest_message_value_dict

    # Method: Seek partitions to latest availiable message
    def seek_topic_partitions_latest(self):
        topic_partitions_dict = self.create_topic_partitions_dict()
        end_offset_topic_partitions_dict = self.create_topic_partitions_end_offsets_dict()

        try:
            # loop topics and seek all partitions to latest availiable message
            for topic in self.topics_consumed_list:
                # loop all partitions for topic
                for topic_partion in topic_partitions_dict[topic]:
                    # if they have messages, per partition
                    if end_offset_topic_partitions_dict[topic][topic_partion] > 0:
                        # seek to highest offset-1
                        partition = topic_partitions_dict[topic][topic_partion.partition]
                        offset = end_offset_topic_partitions_dict[topic][topic_partion]-1
                        self.consumer.seek(partition, offset)
        except Exception as e:
            add_to_log(f"Error: Seeking consumer: '{partition}' to offset: {offset} failed with message '{e}'.")
            sys.exit(1)

    # Method: Get latest message value per topic and return it in dictionary - using poll
    def get_latest_topic_messages_to_dict_poll_based(self):
        # TODO modify this to include timeout for max time spend on polls?

        # init dictionary with Topic -> TopicPartitions
        topic_partitions_dict = self.create_topic_partitions_dict()

        # init dictionary with Topic,TopicPartition -> begin_offset-1 unless 0 (used for tracking last read offset)
        # TODO verify if this works for empty topic
        last_read_offset_topic_partitions_dict = self.create_topic_partitions_last_read_offset_dict()

        # dictionaries for holding latest timestamp and value for each consumed topic
        topic_latest_message_timestamp_dict, topic_latest_message_value_dict = self.create_topic_latest_dicts()

        # Seek all partitions for consumed topics to latest availiable message
        self.seek_topic_partitions_latest()

        # init dictionary with Topic,TopicPartition -> end_offset
        end_offset_topic_partitions_dict = self.create_topic_partitions_end_offsets_dict()

        # poll data
        is_polling = True
        while is_polling:

            data_object = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=None)

            if data_object:

                # loop all messages returned by poll per partition
                for topic_partition in data_object:
                    # loop all messages for partition
                    for msg in range(0, len(data_object[topic_partition])):
                        # if timestamp than last read msg for topic, then update values in dict
                        topic = data_object[topic_partition][msg].topic
                        if data_object[topic_partition][msg].timestamp > topic_latest_message_timestamp_dict[topic]:
                            topic_latest_message_timestamp_dict[topic] = data_object[topic_partition][msg].timestamp
                            topic_latest_message_value_dict[topic] = data_object[topic_partition][msg].value
                        # update last read mesage offset dict
                        last_read_offset_topic_partitions_dict[topic][topic_partition] = data_object[topic_partition][msg].offset

            # Make list of partitions for which last message offset has not yet been reached
            # TODO make af function (maybe sub function?)
            topic_partitions_not_reached_last_offset = []
            for topic in self.topics_consumed_list:
                for topic_partition in topic_partitions_dict[topic]:
                    if last_read_offset_topic_partitions_dict[topic][topic_partition] < end_offset_topic_partitions_dict[topic][topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partions have been consumed till latest offset, break out of polling loop
            if len(topic_partitions_not_reached_last_offset) == 0:
                is_polling = False

        return topic_latest_message_value_dict

    # Method: Get latest message value per topic and return it in dictionary - using consumer loop
    def get_latest_topic_messages_to_dict_loop_based(self):
        # Latest message is determined on timestamp. Will loop all partitions.
        # TODO put everything in a try catch?
        # TODO modify this to include timeout logic?

        # init dictionary with Topic -> TopicPartitions
        topic_partitions_dict = self.create_topic_partitions_dict()

        # init dictionary with Topic,TopicPartition -> begin_offset-1 unless 0 (used for tracking last read offset)
        last_read_offset_topic_partitions_dict = self.create_topic_partitions_last_read_offset_dict()

        # dictionaries for holding latest timestamp and value for each consumed topic
        topic_latest_message_timestamp_dict, topic_latest_message_value_dict = self.create_topic_latest_dicts()

        # Seek all partitions for consumed topics to latest availiable message
        self.seek_topic_partitions_latest()

        # init dictionary with Topic,TopicPartition -> end_offset
        end_offset_topic_partitions_dict = self.create_topic_partitions_end_offsets_dict()

        for message in self.consumer:

            # if timestamp of read message is newer that last read message from topic, update dict
            if message.timestamp > topic_latest_message_timestamp_dict[message.topic]:
                topic_latest_message_timestamp_dict[message.topic] = message.timestamp
                topic_latest_message_value_dict[message.topic] = message.value

            # update last read mesage offset
            last_read_offset_topic_partitions_dict[message.topic][TopicPartition(message.topic, message.partition)] = message.offset

            # Make list of partitions for which last message offset has not yet been reached
            # TODO make function
            topic_partitions_not_reached_last_offset = []
            for topic in self.topics_consumed_list:
                for topic_partition in topic_partitions_dict[topic]:
                    if last_read_offset_topic_partitions_dict[topic][topic_partition] < end_offset_topic_partitions_dict[topic][topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partions have been consumed till latest offset, break out of conusmer loop
            if len(topic_partitions_not_reached_last_offset) == 0:
                break

        return topic_latest_message_value_dict

    # Method: get latest msg from topic an return it
    def get_latest_msg_from_topic(self, topic_name):
        topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_poll_based()

        if topic_latest_message_value_dict[topic_name] is None:
            add_to_log("Error: Message is not avialiable from topic: '{topic_name}'")
            sys.exit(1)
        else:
            message = topic_latest_message_value_dict[topic_name]

        return message

    # Method: get latest message value from topic and return it
    def get_latest_msg_val_from_topic(self, topic_name, msg_val_name, default_msg_val=None, precision=3):
        # TODO doc
        topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_poll_based()

        if topic_latest_message_value_dict[topic_name][msg_val_name] is None:
            add_to_log(f"Warning: Value: {msg_val_name} is not avialiable from topic: " +
                       f"'{topic_name}'. Setting to default value: '{default_msg_val}'.")
            message_value = default_msg_val
        else:
            message_value = topic_latest_message_value_dict[topic_name][msg_val_name]

        if type(message_value) == float:
            message_value = round(message_value, precision)

        return message_value

    # Method: get latest message values from alle consumed topics and return
    def get_latest_msg_from_consumed_topics_to_dict(self):
        # TODO add possibility of setting default value?
        # TODO make possibility of continuiing via flag if topic empty (will return none...)
        topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_poll_based()
        # topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_loop_based()

        empty_topics = []
        for topic in topic_latest_message_value_dict:
            if topic_latest_message_value_dict[topic] is None:
                empty_topics.append(topic)
            if empty_topics:
                add_to_log(f"Warning: No data was availiable on consumed Kafka Topic(s): {empty_topics}.")
                # sys.exit(1)

        return topic_latest_message_value_dict

    # Method: Produce message to topic
    def produce_message(self, topic_name, msg_value):
        # TODO doc
        # TODO verify if topic name is in producer list? (if not then what?)
        try:
            self.producer.send(topic_name, value=msg_value)
            return True
        except Exception as e:
            add_to_log(f"Error: Sending message to Kafka failed with message: '{e}'.")
            sys.exit(1)
