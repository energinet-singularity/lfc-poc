from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from json import dumps, loads
from time import sleep
import sys
from functions_lfc import add_to_log


def init_producer(bootstrap_servers: list):
    """Initializes Kafka producer.

    Arguments:

    Returns:


    """
    try:
        producer_kafka = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: dumps(x).encode('utf-8'))
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
        consumer_kafka = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group_id, value_deserializer=lambda x: loads(x.decode('utf-8')), auto_offset_reset='earliest', enable_auto_commit=False)
        add_to_log("Info: Kafka consumer connection established.")
        return consumer_kafka
    except Exception as e:
        add_to_log(f"Error: Kafka Consumer connection failed with message: '{e}'.")
        sys.exit(1)


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


def topics_exists(consumer: KafkaConsumer, topic_list: list):
    """ Verify if topics exist in Kafka Brooker.

    Arguments:

    Returns:

    """
    error_found = False

    for topic in topic_list:
        # if topic not found by cosumer report it
        try:
            if topic not in consumer.topics():
                add_to_log(f"Error: Topic '{topic}' does not exist.")
                error_found = True
        except Exception as e:
            add_to_log(f"Error: Verifying if topic: '{topic}' exists failed message: '{e}'.")
            sys.exit(1)

    if error_found:
        return False
    else:
        add_to_log("Info: All topics exist.")
        return True


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
            offset_topic_partitions_dict[topic] = consumer.beginning_offsets([TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)])
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
            offset_topic_partitions_dict[topic] = consumer.end_offsets([TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)])
        return offset_topic_partitions_dict
    except Exception as e:
        add_to_log(f"Error: Getting end offset for partitions for topic: '{topic}' failed with message: '{e}'.")
        sys.exit(1)


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


def get_latest_topic_messages_to_dict(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    """ Get latest message value per topic and return it in dictionary - using consumer loop

    Arguments

    Returns:

    """
    # TODO put everything in a try catch
    # TODO modify this to include timeout og retry logic
    # TODO will seek to latest message, latest is indentified at startup (more messages can come durring runtime, how to take into account message arriving faster than read?)

    #
    topic_partitions_dict = create_topic_partitions_dict(consumer=consumer, topic_list=topic_list)

    # TODO rename til noget med "current"
    first_offset_topic_partitions_dict = create_topic_partitions_begin_offsets_dict(consumer=consumer, topic_list=topic_list)

    # dictionary for holding latest timestamp and value for each consumed topic
    # TODO make as function
    topic_latest_message_timestamp_dict = {}
    topic_latest_message_value_dict = {}

    # init dictionarys for all topics
    for topic in topic_list:
        topic_latest_message_timestamp_dict[topic] = 0
        topic_latest_message_value_dict[topic] = None

    # Seek all partitions for consumed topics to latest availiable message
    if not seek_topic_partitions_latest(consumer=consumer, topic_list=topic_list):
        sys.exit(1)

    #
    last_offset_dict = create_topic_partitions_end_offsets_dict(consumer=consumer, topic_list=topic_list)

    for message in consumer:

        # add_to_log(f"Message: '{message.value}' has offset: {message.offset}")

        #
        first_offset_topic_partitions_dict[message.topic][TopicPartition(message.topic, message.partition)] = message.offset

        #
        if message.timestamp > topic_latest_message_timestamp_dict[message.topic]:
            topic_latest_message_timestamp_dict[message.topic] = message.timestamp
            topic_latest_message_value_dict[message.topic] = message.value

        #
        consumer.commit()

        #
        topic_partitions_not_reached_last_offset = []

        #
        for topic in topic_list:
            for topic_partition in topic_partitions_dict[topic]:

                if first_offset_topic_partitions_dict[topic][topic_partition] < last_offset_dict[topic][topic_partition]-1:
                    topic_partitions_not_reached_last_offset.append(topic_partition)

        #
        count_part = len(topic_partitions_not_reached_last_offset)

        #
        if count_part == 0:
            break
        # else hvis alle data

    # Verify if data was availiable
    for topic in topic_list:
        if topic_latest_message_value_dict[topic] is None:
            add_to_log(f"Info: No data was availiable on consumed Kafka Topic: '{topic}', value set to: 'None'.")

    return topic_latest_message_value_dict


def get_latest_topic_messages_to_dict_poll_based(consumer: KafkaConsumer, topic_list: list, timeout_ms: int):
    """ Get latest message value per topic and return it in dictionary - using .poll

    Arguments:

    Returns:

    """
    # TODO modify this to use timeout instead of amount of polls?

    # Seek all partitions for consumed topics to latest availiable message
    if not seek_topic_partitions_latest(consumer=consumer, topic_list=topic_list):
        sys.exit(1)

    poll_count = 0
    poll_max = 10

    # dictionary for holding latest timestamp and value for each consumed topic
    topic_latest_message_timestamp_dict = {}
    topic_latest_message_value_dict = {}

    # init dictionarys for all topics
    for topic in topic_list:
        topic_latest_message_timestamp_dict[topic] = 0
        topic_latest_message_value_dict[topic] = None

    # topic partition dict
    topic_partitions_dict = create_topic_partitions_dict(consumer=consumer, topic_list=topic_list)

    is_polling = True

    # poll data
    while is_polling:

        data_object = consumer.poll(timeout_ms=timeout_ms, max_records=None)
        poll_count += 1

        if data_object:

            # loop all topics and loop partitions for each and find latest value based on timestamp
            for topic in topic_list:
                # loop all partitions for topic
                for topic_partion in topic_partitions_dict[topic]:
                    # if dataobject contain data for TopicPartition
                    if topic_partion in data_object:
                        # loop all consumer records for TopicPartition
                        for x in range(0, len(data_object[topic_partion])):
                            # if timestamp is latest then update values in dict
                            if data_object[topic_partion][x].timestamp > topic_latest_message_timestamp_dict[topic]:
                                topic_latest_message_timestamp_dict[topic] = data_object[topic_partion][x].timestamp
                                topic_latest_message_value_dict[topic] = data_object[topic_partion][x].value
        else:
            if poll_count > poll_max:
                is_polling = False

        # TOTO use timeout and max polls instead
        sleep(0.001)

    # Verify if data was availiable
    for topic in topic_list:
        if topic_latest_message_value_dict[topic] is None:
            add_to_log(f"Warning: No data was availiable on consumed Kafka Topic: '{topic}', value set to 'None'.")

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
