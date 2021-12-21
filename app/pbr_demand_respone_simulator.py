from time import sleep, time
import sys
import os
from functions_kafka import init_producer, init_consumer, init_topic_partitions, produce_message, subscribe_topics, topics_exists, get_latest_topic_messages_to_dict, get_latest_topic_messages_to_dict_poll_based
from functions_lfc import add_to_log, simulate_pbr_response

# importing settings
import lfc_kafka_topic_names as tp_nm
import lfc_kafka_message_value_names as msg_val_nm
import lfc_parameters as PARM

# kafka brooker settings
kafka_host = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")
kafka_port = os.environ.get('KAFKA_PORT', "9092")

if ':' not in kafka_host and kafka_port != "":
    kafka_host += f":{kafka_port}"

# Start...
add_to_log("Info: LFC demand response simulation initializing.")

# Kafka consumergroup name
consumer_gp_nm = "lfc_demand_response_simu"

# list of topic names (produced to, consumed from and combined list)
topics_produced_list = [tp_nm.lfc_p_dem, tp_nm.lfc_pbr_response]
topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_mw_diff, tp_nm.lfc_pbr_response]
topics_list = list(set(topics_produced_list + topics_consumed_list))

# dictionary for holding latest message value for each consumed topic
topic_latest_message_value_dict = {}

# Kafka producer init
producer_kafka = init_producer(bootstrap_servers=kafka_host)

# Kafka consumer
consumer_kafka = init_consumer(bootstrap_servers=kafka_host, group_id=consumer_gp_nm, auto_offset_reset='earliest', enable_auto_commit=False)

# subscribe to topics
subscribe_topics(consumer=consumer_kafka, topic_list=topics_consumed_list)

# dummy poll needed for partiontions to be assigned
init_topic_partitions(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

add_to_log("Simulating LFC Demand response....")
add_to_log("|-------------------------------------------------|")

# Verify if all needed topics exist in kafka brooker
# TODO is slowish. keep here or add in loop to verify if topics exist durring runtime or just let it exit and thereby restart container
if not topics_exists(consumer=consumer_kafka, topic_list=topics_consumed_list):
    sys.exit(1)

while True:

    time_pbr_simu_loop_start = time()

    # TODO verify here if kafka brooker is availiable or just exit and thereby restart container
    # TODO verify if topics are subscribed (is i necessary to check durring runtime)
    # TODO Verify if partitions assigned or just exit and thereby restart container
    # TODO Log time consumption as info

    # Getting latest value for each topic
    time_message_get_start = time()
    # topic_latest_message_value_dict = get_latest_topic_messages_to_dict(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)
    topic_latest_message_value_dict = get_latest_topic_messages_to_dict_poll_based(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)
    add_to_log(f"Getting messages took: {round(time()-time_message_get_start,3)} secounds.")

    # extract value, topic specific, and round to decimals defined by precision varialbe
    # TODO simplify/make smarter
    if topic_latest_message_value_dict[tp_nm.lfc_p_target] is None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_p_target} is not avialiable from topic: '{tp_nm.lfc_p_target}'. Setting to zero.")
        current_lfc_p_target = 0
    else:
        # TODO byg sikring mod forkert data value name
        current_lfc_p_target = round(topic_latest_message_value_dict[tp_nm.lfc_p_target][msg_val_nm.lfc_p_target], PARM.PRECISION_DECIMALS)

    if topic_latest_message_value_dict[tp_nm.lfc_mw_diff] is None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_mw_diff} is not avialiable from topic: '{tp_nm.lfc_mw_diff}'. Setting to zero.")
        current_lfc_mw_diff = 0
    else:
        current_lfc_mw_diff = round(topic_latest_message_value_dict[tp_nm.lfc_mw_diff][msg_val_nm.lfc_mw_diff], PARM.PRECISION_DECIMALS)

    if topic_latest_message_value_dict[tp_nm.lfc_pbr_response] is None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_pbr_response} is not avialiable from topic: '{tp_nm.lfc_pbr_response}'. Setting to zero.")
        last_pbr_response = 0
    else:
        last_pbr_response = round(topic_latest_message_value_dict[tp_nm.lfc_pbr_response][msg_val_nm.lfc_pbr_response], PARM.PRECISION_DECIMALS)

    add_to_log(f"Current target is: {current_lfc_p_target}")
    add_to_log(f"MW diff is: {current_lfc_mw_diff}")

    # simulate PBR responce
    response_pbr = simulate_pbr_response(p_target=current_lfc_p_target, last_pbr_response=last_pbr_response)

    # send current pbr repsonce to kafka topic
    produce_message(producer=producer_kafka, topic_name=tp_nm.lfc_pbr_response, value={msg_val_nm.lfc_pbr_response: response_pbr})

    # send system responce (sum of mw diff and PBR response) to kafka topic
    response_system = round(current_lfc_mw_diff+response_pbr, PARM.PRECISION_DECIMALS)
    produce_message(producer=producer_kafka, topic_name=tp_nm.lfc_p_dem, value={msg_val_nm.lfc_p_dem: response_system})
    add_to_log(f"System response: {response_system} was send as new LFC demand")

    add_to_log(f"Loop took: {round(time()-time_pbr_simu_loop_start,3)} secounds.")
    add_to_log("|-------------------------------------------------|")
    sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
