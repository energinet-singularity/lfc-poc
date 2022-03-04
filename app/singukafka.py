from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import dumps, loads
import pandas as pd
from time import time
import sys
import os
import logging

# Initialize log
log = logging.getLogger(__name__)


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
                 poll_timeout_ms=100,
                 fetch_max_wait_ms=200):

        # attributes set on object instansiation, either by default or supplied values.
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.topics_consumed_list = topics_consumed_list
        self.topics_produced_list = topics_produced_list
        self.poll_timeout_ms = poll_timeout_ms
        self.fetch_max_wait_ms = fetch_max_wait_ms

        # attributes set based on others
        self.topic_list = list(set(self.topics_produced_list + self.topics_consumed_list))
        self.topics_consumed_only = list(set(self.topics_consumed_list) - set(self.topics_produced_list))
        self.topics_produced_only = list(set(self.topics_produced_list) - set(self.topics_consumed_list))
        self.topics_consumed_and_produced = list(set(self.topics_consumed_list).intersection(self.topics_produced_list))

        # attributes init by methods on creation
        self.bootstrap_servers = None
        self.consumer = None
        self.producer = None

        # methods called to init attributes
        self.set_kafka_brooker_from_env()
        self.init_consumer()
        self.verify_topic_existence()
        if topics_consumed_list:
            self.subscribe_topics()
        if topics_produced_list:
            self.init_producer()

        # TODO move this init to seek function only ? (DUMMY POLL INIT, only call once or jsut always call before seek?)
        self.init_topic_partitions()

    # Method: set kafka brooker from ENV or default to value
    def set_kafka_brooker_from_env(self):
        log.debug("Setting kafka boostrap servers from environment variable.")
        try:
            self.bootstrap_servers = os.environ.get('KAFKA_HOST', "my-cluster-kafka-bootstrap.kafka")
            log.info(f"Kakfa bootsrap servers was set to: {self.bootstrap_servers}")
        except Exception as e:
            log.exception(f"Setting kafka boostrap servers from environment variable failed with message: '{e}'.")
            sys.exit(1)

    # Method: Initializes Kafka comsumer.
    def init_consumer(self):
        log.debug("Kafka consumer connection initializing.")
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                          group_id=self.group_id,
                                          value_deserializer=lambda x: loads(x.decode('utf-8')),
                                          auto_offset_reset=self.auto_offset_reset,
                                          enable_auto_commit=self.enable_auto_commit,
                                          fetch_max_wait_ms=self.fetch_max_wait_ms)
            log.info("Kafka consumer connection established.")
        except Exception as e:
            log.exception(f"Kafka Consumer connection failed with message: '{e}'.")
            sys.exit(1)

    # Method: Initializes Kafka producer.
    def init_producer(self):
        log.debug("Kafka producer connection initializing.")
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                          value_serializer=lambda x: dumps(x).encode('utf-8'),
                                          key_serializer=lambda x: dumps(x).encode('utf-8'))
            log.info("Kafka producer connection established.")
        except Exception as e:
            log.exception(f"Kafka producer connection failed with message: '{e}'.")
            sys.exit(1)

    # Method: Dummy Poll, which is needed to force assignment of partitions after subsribtion to topics. (only needed when using seek method)
    def init_topic_partitions(self):
        log.debug("Initiliasing topic partitions assignemnt.")
        # TODO check if using a dummy poll is the correct way, maybe also consumer loop could work?
        # TODO handle that when making dummy poll, if auto comit is enabled, then the offset will shift unessecary? (fix by using position and seek)
        try:
            self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=1)
            log.debug("Initial poll done. TopicPartitions are now assigned.")
        except Exception as e:
            log.exception(f"Initial poll failed with message '{e}'.")
            sys.exit(1)
        """
        if len(kafka_obj.list_empty_consumed_only_topics()) == 0:
            try:
                for message in kafka_obj.consumer:
                    break
                kafka_obj.consumer.seek(partition=TopicPartition(message.topic, message.partition),offset=message.offset)
                log.info("TopicPartitions assigned via loop.")
            except Exception as e:
                log.exception(f"?? failed with message '{e}'.")
                sys.exit(1)
        else:
            try:
                data = kafka_obj.consumer.poll()

                if self.auto_offset_reset == 'earliest':
                    self.consumer.seek_to_beginning()
                elif self.auto_offset_reset == 'latest':
                    # TODO handle this how? (via position som gemmes før poll)
                    pass

                log.info("TopicPartitions assigned via Poll.")

            except Exception as e:
                log.exception(f"Initial poll failed with message '{e}'.")
                sys.exit(1)


        # kafka_obj.seek_topic_partitions_latest()
        """

    # Method: Subscribes consumer to topics
    def subscribe_topics(self):
        log.debug("Subscribing to consumed kafka topics.")
        try:
            self.consumer.subscribe(self.topics_consumed_list)
            log.info(f"Kafka consumer subscribed to topics: '{self.topics_consumed_list}'.")
        except Exception as e:
            log.info(f"Kafka consumer subscribtion to topics: '{self.topics_consumed_list}' " +
                     f"failed with message: '{e}'.")
            sys.exit(1)

    # Method: Verify if topics exists.
    def verify_topic_existence(self):
        log.debug("Verifying if topics {self.topic_list} exist via Kakfa brooker.")
        unavbl_topics = []
        for topic in self.topic_list:
            try:
                if topic not in self.consumer.topics():
                    unavbl_topics.append(topic)
            except Exception as e:
                log.exception(f"Verifying if topic: '{topic}' exists failed with message: '{e}'.")
                sys.exit(1)

        if unavbl_topics:
            log.error(f"The Topic(s): {unavbl_topics} does not exist.")
            sys.exit(1)
        else:
            log.debug("All topics exist.")

    # Method: Create a dictionary with partitions availiable for each topic.
    def create_topic_partitions_dict(self):
        log.debug("Creating topic partitions dictionary")
        if type(self.topic_list) != list:
            log.error(f"Supplied topics must have the type 'list' but is {type(self.topic_list)}")
            sys.exit(1)

        topic_partitions_dict = {}
        try:
            for topic in self.topic_list:
                topic_partitions_dict[topic] = [TopicPartition(topic, p)
                                                for p in self.consumer.partitions_for_topic(topic)]
            log.debug("Created topic partitions dictionary")
        except Exception as e:
            log.exception(f"Making dictionary of lists for topic partitions for topic: '{topic}' " +
                          f"failed with message: '{e}'.")
            sys.exit(1)

        return topic_partitions_dict

    # Method: Create a dictionary with topic partition --> begin offsets
    def create_topic_partitions_begin_offsets_dict(self):
        log.debug("Creating dictionary with: topic partitions -> begin offsets.")
        begin_offset_topic_partitions_dict = {}

        try:
            for topic in self.topic_list:
                begin_offset_topic_partitions_dict[topic] = self.consumer.beginning_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
            log.debug("Created dictionary.")
        except Exception as e:
            log.exception(f"Getting latest offset for partitions for topic: '{topic}' failed with message '{e}'.")
            sys.exit(1)

        return begin_offset_topic_partitions_dict

    # Method: Create a dictionary with topic partition --> end offsets
    def create_topic_partitions_end_offsets_dict(self):
        log.debug("Creating dictionary with: topic partitions -> end offsets.")
        end_offset_topic_partitions_dict = {}

        try:
            for topic in self.topic_list:
                end_offset_topic_partitions_dict[topic] = self.consumer.end_offsets([TopicPartition(topic, p) for p in self.consumer.partitions_for_topic(topic)])
            log.debug("Created dictionary.")
        except Exception as e:
            log.exception(f"Getting end offset for partitions for topic: '{topic}' failed with message: '{e}'.")
            sys.exit(1)

        return end_offset_topic_partitions_dict

    # Method: Create a dictionary with topic partition --> last read offset
    def create_topic_partitions_last_read_offset_dict(self):
        log.debug("Creating dictionary with: topic partitions -> last read offsets.")
        # TODO use position instead?
        # TODO verify if this works for empty topic (works with minus 1?)
        # TODO make with position instead

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

    # Method: list empty topics
    def list_empty_topics(self, topic_list):
        topic_partitions_dict = self.create_topic_partitions_dict()
        begin_offset_topic_partitions_dict = self.create_topic_partitions_begin_offsets_dict()
        end_offset_topic_partitions_dict = self.create_topic_partitions_end_offsets_dict()

        empty_topics = topic_list.copy()

        for topic in topic_list:
            # loop all partitions for topic
            for topic_partition in topic_partitions_dict[topic]:
                if end_offset_topic_partitions_dict[topic][topic_partition] > begin_offset_topic_partitions_dict[topic][topic_partition]:
                    empty_topics.remove(topic)

        return empty_topics

    # Method: list empty consumed only topics
    def list_empty_consumed_only_topics(self):
        return self.list_empty_topics(self.topics_consumed_only)

    # Method: list empty produced only topics
    def list_empty_produced_only_topics(self):
        return self.list_empty_topics(self.topics_produced_only)

    # Method: list empty consuemd and produced topics
    def list_empty_consumed_and_produced_topics(self):
        return self.list_empty_topics(self.topics_consumed_and_produced)

    # Method: Seek partitions to latest availiable message
    def seek_topic_partitions_latest(self):

        topic_partitions_dict = self.create_topic_partitions_dict()
        end_offset_topic_partitions_dict = self.create_topic_partitions_end_offsets_dict()

        try:
            # loop topics and seek all partitions to latest availiable message
            for topic in self.topics_consumed_list:
                # loop all partitions for topic
                for topic_partition in topic_partitions_dict[topic]:
                    # if they have messages, per partition
                    if end_offset_topic_partitions_dict[topic][topic_partition] > 0:
                        # seek to highest offset-1
                        partition = topic_partitions_dict[topic][topic_partition.partition]
                        offset = end_offset_topic_partitions_dict[topic][topic_partition]-1
                        self.consumer.seek(partition, offset)
        except Exception as e:
            log.exception(f"Seeking consumer: '{partition}' to offset: {offset} failed with message '{e}'.")
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
            # TODO use funciton topic_positions_reached_last_offsets og slet last_read_offset_topic_partitions_dict?
            topic_partitions_not_reached_last_offset = []
            for topic in self.topics_consumed_list:
                for topic_partition in topic_partitions_dict[topic]:
                    # if self.consumer.position(topic_partition) < self.consumer.end_offsets([topic_partition])[topic_partition]-1:
                    if last_read_offset_topic_partitions_dict[topic][topic_partition] < end_offset_topic_partitions_dict[topic][topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partitions have been consumed till latest offset, break out of polling loop
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
            # TODO use funciton topic_positions_reached_last_offsets og slet last_read_offset_topic_partitions_dict?
            topic_partitions_not_reached_last_offset = []
            for topic in self.topics_consumed_list:
                for topic_partition in topic_partitions_dict[topic]:
                    if last_read_offset_topic_partitions_dict[topic][topic_partition] < end_offset_topic_partitions_dict[topic][topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partitions have been consumed till latest offset, break out of conusmer loop
            if len(topic_partitions_not_reached_last_offset) == 0:
                break

        return topic_latest_message_value_dict

    # Method: get latest msg from topic an return it
    def get_latest_msg_from_topic(self, topic_name):
        topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_poll_based()

        if topic_latest_message_value_dict[topic_name] is None:
            log.exception("Message is not avialiable from topic: '{topic_name}'")
            sys.exit(1)
        else:
            message = topic_latest_message_value_dict[topic_name]

        return message

    # Method: get latest message value from topic and return it
    def get_latest_msg_val_from_topic(self, topic_name, msg_val_name, default_msg_val=None, precision=3):
        # TODO doc
        topic_latest_message_value_dict = self.get_latest_topic_messages_to_dict_poll_based()

        if topic_latest_message_value_dict[topic_name][msg_val_name] is None:
            log.warning(f"Value: {msg_val_name} is not avialiable from topic: " +
                        f"'{topic_name}'. Setting to default value: '{default_msg_val}'.")
            message_value = default_msg_val
        else:
            message_value = topic_latest_message_value_dict[topic_name][msg_val_name]

        if type(message_value) == float:
            message_value = round(message_value, precision)

        return message_value

    # Method: Produce message to topic
    def produce_message(self, topic_name, msg_value, msg_key="NA"):
        # TODO doc
        # TODO verify if topic name is in producer list? (if not then what?)

        try:
            self.producer.send(topic_name, value=msg_value, key=msg_key)
            return True
        except Exception as e:
            log.exception(f"Sending message to Kafka failed with message: '{e}'.")
            sys.exit(1)

    # Method: latest kafka to Pandas
    # TODO: add such that it updates latest msg to attribute, then make function which extract from it and recalls it
    # TODO: build in timeout to avopid endless polling?
    # todo lav try/catch
    def get_kafka_messages_to_pandas_dataframe(self, msg_key_filter=None, get_latest_msg_by_key=False, get_latest_produced_msg_only=True):
        time_begin = time()

        if not get_latest_produced_msg_only:
            self.consumer.seek_to_beginning()

        # data lists
        msg_topic = []
        msg_partition = []
        msg_offset = []
        msg_timestamp = []
        msg_key = []
        msg_val = []

        # poll data
        is_polling = True
        while is_polling:

            data_object = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=None)

            if data_object:

                # loop all messages returned by poll per partition
                for topic_partition in data_object:
                    # loop all messages for partition
                    for msg in range(0, len(data_object[topic_partition])):
                        # TODO check if looping is needed, cant it be parsed smarter
                        msg_topic.append(data_object[topic_partition][msg].topic)
                        msg_partition.append(data_object[topic_partition][msg].partition)
                        msg_offset.append(data_object[topic_partition][msg].offset)
                        msg_timestamp.append(data_object[topic_partition][msg].timestamp)

                        encoded_key = data_object[topic_partition][msg].key
                        if encoded_key is None:
                            decoded_key = "NA"
                        else:
                            decoded_key = loads(data_object[topic_partition][msg].key.decode())

                        msg_key.append(decoded_key)
                        msg_val.append(data_object[topic_partition][msg].value)

            # check if all topic has been polled
            if self.topic_positions_reached_last_offsets(topic_list=self.topics_consumed_list):
                is_polling = False

        # make dataframe structure
        data_dict = {'topic': msg_topic, 'partition': msg_partition, 'offset': msg_offset, 'timestamp': msg_timestamp, 'key': msg_key, 'value': msg_val}
        dataframe = pd.DataFrame.from_dict(data_dict)

        # filter if choosen
        if msg_key_filter is not None:
            dataframe = self.filter_kafaka_data_frame_by_msg_key(dataframe=dataframe, msg_key_filter=msg_key_filter, filter_on_latest_msg_by_key=get_latest_msg_by_key)

        log.debug(f"kafka to panda took {time()-time_begin}")
        return dataframe

    # todo lav try/catch
    def filter_kafaka_data_frame_by_msg_key(self, dataframe, msg_key_filter, filter_on_latest_msg_by_key=False):
        # filter by key
        dataframe = dataframe[dataframe.key == msg_key_filter]
        if filter_on_latest_msg_by_key:
            # filter by latest offset
            dataframe = dataframe[dataframe.offset == dataframe[dataframe.key == msg_key_filter]["offset"].max()]

        return dataframe

    # todo try catch and dok
    def topic_positions_reached_last_offsets(self, topic_list):
        
        try:
            # init dictionary with Topic -> TopicPartitions
            topic_partitions_dict = self.create_topic_partitions_dict()
            topic_partitions_not_reached_last_offset = []
            for topic in topic_list:
                for topic_partition in topic_partitions_dict[topic]:
                    if self.consumer.position(topic_partition) < self.consumer.end_offsets([topic_partition])[topic_partition]-1:
                        topic_partitions_not_reached_last_offset.append(topic_partition)

            # If all partitions have been consumed till latest offset, break out of polling loop
            if len(topic_partitions_not_reached_last_offset) == 0:
                return True
            else:
                return False
        except Exception as e:
            log.exception(f"Checking for last offset for: '{topic_partition}' failed with message '{e}'.")
            sys.exit(1)
        
