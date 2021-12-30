from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import dumps, loads
import sys
import os
from functions_lfc import add_to_log


def set_kafka_brooker_from_env():
    """

    """
    kafka_host = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")
    kafka_port = os.environ.get('KAFKA_PORT', "9093")

    if ':' not in kafka_host and kafka_port != "":
        kafka_host += f":{kafka_port}"

    return kafka_host


def init_producer(bootstrap_servers: list):
    """Initializes Kafka producer.

    Arguments:

    Returns:


    """
    try:
        producer_kafka = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))
        add_to_log("Info: Kafka producer connection established.")
        return producer_kafka
    except Exception as e:
        add_to_log(f"Error: Kafka producer connection failed with message: '{e}'.")
        sys.exit(1)


def init_consumer(bootstrap_servers: list, group_id: str, auto_offset_reset: str, enable_auto_commit: str):
    """Initializes Kafka comsumer, consuming from beginning af partitions.

    Arguments:

    Returns:

    """
    try:
        consumer_kafka = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                       group_id=group_id,
                                       value_deserializer=lambda x: loads(x.decode('utf-8')),
                                       auto_offset_reset='earliest', enable_auto_commit=False)
        add_to_log("Info: Kafka consumer connection established.")
        return consumer_kafka
    except Exception as e:
        add_to_log(f"Error: Kafka Consumer connection failed with message: '{e}'.")
        sys.exit(1)


def init_topic_partitions(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    """Dummy Poll, which is needed to force assignment of partitions after subsribtion to topics.

    Arguments:

    Returns:

    """

    # TODO check if using a dummy poll is the correct way
    # TODO handle that when making dummy poll, if auto comit is enabled, then the offset will shift unessecary
    try:
        consumer.poll(timeout_ms=timeout_ms)
        add_to_log("Info: Initial poll done. TopicPartitions are now assigned.")
    except Exception as e:
        add_to_log(f"Error: Initial poll failed with message '{e}'.")
        sys.exit(1)


def init_topic_latest_msg_dicts(topic_list: list):
    """

    """
    topic_latest_message_timestamp_dict = {}
    topic_latest_message_value_dict = {}
    for topic in topic_list:
        topic_latest_message_timestamp_dict[topic] = 0
        topic_latest_message_value_dict[topic] = None
    return topic_latest_message_timestamp_dict, topic_latest_message_value_dict


def subscribe_topics(consumer: KafkaConsumer, topic_list: list):
    """
    Subscribes consumer to topics

    Arguments:

    Returns:

    """
    try:
        consumer.subscribe(topic_list)
        add_to_log(f"Info: Kafka consumer subscribed to topics: '{topic_list}'.")
    except Exception as e:
        add_to_log(f"Error: Kafka consumer subscribtion to topics: '{topic_list}' failed with message: '{e}'.")
        sys.exit(1)


def list_unavbl_topics(consumer: KafkaConsumer, topic_list: list, print_err: bool):
    """ Verify if topics exist in Kafka Brooker.
    Return list of topics which were not found.
    Arguments:

    Returns:
    List of unavailiavle
    """
    unavbl_topics = []
    for topic in topic_list:
        try:
            if topic not in consumer.topics():
                unavbl_topics.append(topic)
        except Exception as e:
            add_to_log(f"Error: Verifying if topic: '{topic}' exists failed message: '{e}'.")
            sys.exit(1)

    if unavbl_topics:
        add_to_log(f"Error: The Topic(s): {unavbl_topics} does not exist.")

    return unavbl_topics


def list_empty_topics(topic_latest_message_value_dict: dict, print_warn: bool):
    """

    """
    empty_topics = []
    for topic in topic_latest_message_value_dict:
        if topic_latest_message_value_dict[topic] is None:
            empty_topics.append(topic)

    if print_warn:
        if empty_topics:
            add_to_log(f"Warning: No data was availiable on consumed Kafka Topic(s): {empty_topics}.")

    return empty_topics


def create_topic_partitions_dict(consumer: KafkaConsumer, topic_list: list):
    """ Create a dictionary with partions avlaiable for each topic.

    Arguments:

    Returns:

    """
    if type(topic_list) != list:
        add_to_log(f"Error: Supplied topics must have the type 'list' but is {type(topic_list)}")
        sys.exit(1)

    topic_partitions_dict = {}
    try:
        for topic in topic_list:
            topic_partitions_dict[topic] = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
        return topic_partitions_dict
    except Exception as e:
        add_to_log(f"Error: Making dictionary of lists for topic partitions for topic: '{topic}' failed with message: '{e}'.")
        sys.exit(1)


def create_topic_partitions_begin_offsets_dict(consumer: KafkaConsumer, topic_list: list):
    """ Create a dictionary with topic partion --> begin offsets

    Arguments:

    Returns:

    """
    offset_topic_partitions_dict = {}
    try:
        for topic in topic_list:
            offset_topic_partitions_dict[topic] = consumer.beginning_offsets([TopicPartition(topic, p)
                                                                             for p in consumer.partitions_for_topic(topic)])
        return offset_topic_partitions_dict
    except Exception as e:
        add_to_log(f"Error: Getting latest offset for partitions for topic: '{topic}' failed with message '{e}'.")
        sys.exit(1)


def create_topic_partitions_end_offsets_dict(consumer: KafkaConsumer, topic_list: list):
    """ Create a dictionary with topic partion --> end offsets

    Arguments:

    Returns:

    """
    offset_topic_partitions_dict = {}
    try:
        for topic in topic_list:
            offset_topic_partitions_dict[topic] = consumer.end_offsets([TopicPartition(topic, p)
                                                                       for p in consumer.partitions_for_topic(topic)])
        return offset_topic_partitions_dict
    except Exception as e:
        add_to_log(f"Error: Getting end offset for partitions for topic: '{topic}' failed with message: '{e}'.")
        sys.exit(1)


def create_topic_partitions_last_read_offset_dict(consumer: KafkaConsumer, topic_list: list):
    """

    """
    topic_partitions_dict = create_topic_partitions_dict(consumer=consumer, topic_list=topic_list)

    last_read_offset_topic_partition = create_topic_partitions_begin_offsets_dict(consumer=consumer, topic_list=topic_list)

    for topic in topic_list:
        for topic_partition in topic_partitions_dict[topic]:
            begin_offset = last_read_offset_topic_partition[topic][topic_partition]
            if begin_offset != 0:
                last_read_offset_topic_partition[topic][topic_partition] = begin_offset-1

    return last_read_offset_topic_partition


def seek_topic_partitions_latest(consumer: KafkaConsumer, topic_list: list):
    """ Seek partitions to latest availiable message

    Arguments:

    Returns:

    """

    topic_partitions_dict = create_topic_partitions_dict(consumer=consumer, topic_list=topic_list)
    offset_topic_partitions_dict = create_topic_partitions_end_offsets_dict(consumer=consumer, topic_list=topic_list)

    try:
        # loop topics and seek all partitions to latest availiable message
        for topic in topic_list:
            # loop all partitions for topic
            for topic_partion in topic_partitions_dict[topic]:
                # if they have messages, per partition
                if offset_topic_partitions_dict[topic][topic_partion] > 0:
                    # seek to highest offset
                    partition = topic_partitions_dict[topic][topic_partion.partition]
                    offset = offset_topic_partitions_dict[topic][topic_partion]-1
                    consumer.seek(partition, offset)
        return True
    except Exception as e:
        add_to_log(f"Error: Seeking consumer: '{partition}' to offset: {offset} failed with message '{e}'.")
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


def get_latest_topic_messages_to_dict_loop_based(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    """ Get latest message value per topic and return it in dictionary - using consumer loop

    Latest message is determined on timestamp. Will loop all partitions.

    Arguments

    Returns:

    """
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


def get_latest_topic_messages_to_dict_poll_based(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    """ Get latest message value per topic and return it in dictionary - using .poll

    Arguments:

    Returns:
    Dict with data. if no data then none is data value

    """
    # TODO modify this to include timeout for max time spend on polls?

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

    # poll data
    is_polling = True
    while is_polling:

        data_object = consumer.poll(timeout_ms=timeout_ms, max_records=None)

        if data_object:

            # loop all messages returned by poll per partition
            for topic_partition in data_object:
                # loop all messages for partition
                for msg in range(0, len(data_object[topic_partition])):
                    # if timestamp is latest then update values in dict
                    topic = data_object[topic_partition][msg].topic
                    if data_object[topic_partition][msg].timestamp > topic_latest_message_timestamp_dict[topic]:
                        topic_latest_message_timestamp_dict[topic] = data_object[topic_partition][msg].timestamp
                        topic_latest_message_value_dict[topic] = data_object[topic_partition][msg].value
                    # update last read mesage offset dict
                    last_read_offset_topic_partition[topic][topic_partition] = data_object[topic_partition][msg].offset

        # Make list of partitions for which last message offset has not yet been reached
        # TODO function?
        topic_partitions_not_reached_last_offset = []
        for topic in topic_list:
            for topic_partition in topic_partitions_dict[topic]:
                if last_read_offset_topic_partition[topic][topic_partition] < last_offset_dict[topic][topic_partition]-1:
                    topic_partitions_not_reached_last_offset.append(topic_partition)

        # If all partions have been consumed till latest offset, break out of conusmer loop
        count_part = len(topic_partitions_not_reached_last_offset)
        if count_part == 0:
            is_polling = False

    return topic_latest_message_value_dict


def produce_message(producer: KafkaProducer, topic_name: str, value: bytes):
    """ Producer

    """
    try:
        producer.send(topic_name, value=value)
        return True
    except Exception as e:
        add_to_log(f"Error: Sending message to Kafka failed with message: '{e}'.")
        sys.exit(1)
