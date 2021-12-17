from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from time import sleep
from datetime import datetime
from json import loads, dumps
import sys
import os
from kafka_helper_functions import add_to_log, init_producer, init_consumer, init_topic_partitions, subscribe_topics, topics_exists, seek_topic_partitions_latest, get_latest_topic_messages_to_dict

# importing settings
import lfc_kafka_topic_names as tp_nm
import lfc_kafka_message_value_names as msg_val_nm
import lfc_parameters as PARM

# kafka brooker settings
kafka_host = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")
kafka_port = os.environ.get('KAFKA_PORT', "9092")

if not ':' in kafka_host and kafka_port != "": kafka_host += f":{kafka_port}"

# Start...
add_to_log(f"Info: LFC demand response simulation initializing.")

# Kafka consumergroup name
consumer_gp_nm = "lfc_demand_response_simu"

# list of topic names (produced to, consumed from and combined list)
topics_produced_list = [tp_nm.lfc_p_dem,tp_nm.lfc_pbr_response]
topics_consumed_list = [tp_nm.lfc_p_target,tp_nm.lfc_mw_diff,tp_nm.lfc_pbr_response]
topics_list = list(set(topics_produced_list + topics_consumed_list))

# dictionary for holding latest message value for each consumed topic
topic_latest_message_value_dict = {}

# Kafka producer init
producer_kafka = init_producer(bootstrap_servers=kafka_host)


# Kafka consumer
consumer_kafka = init_consumer(bootstrap_servers=kafka_host, group_id=consumer_gp_nm,auto_offset_reset='earliest', enable_auto_commit=False)

# subscribe to topics
subscribe_topics(consumer=consumer_kafka, topic_list=topics_consumed_list)

#dummy poll needed for partiontions to be assigned
init_topic_partitions(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

add_to_log(f"Simulating LFC Demand response....")
add_to_log(f"|-------------------------------------------------|")

# Verify if all needed topics exist in kafka brooker 
# xxx is slowish. keep here or add in loop?
if not topics_exists(consumer=consumer_kafka, topic_list=topics_consumed_list): sys.exit(1)

while True:

    # xxx check om kafka brookeren er klar, ellers retry og exit?
    # xxx verify om topics er subscribed
    # xxx Verify if partiotions assigned hvordan?
    # xxx log tidforbrug per run og tjeck det
        
    # Getting latest value for each topic
    topic_latest_message_value_dict = get_latest_topic_messages_to_dict(consumer=consumer_kafka,topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

    # extract value, topic specific, and round to decimals defined by precision varialbe
    if topic_latest_message_value_dict[tp_nm.lfc_p_target] == None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_p_target} is not avialiable from topic: '{tp_nm.lfc_p_target}'. Setting to zero.")
        current_lfc_p_target = 0
    else: #xxx byg sikring mod forkert data value name
        current_lfc_p_target = round(topic_latest_message_value_dict[tp_nm.lfc_p_target][msg_val_nm.lfc_p_target],PARM.PRECISION_DECIMALS)
    
    if topic_latest_message_value_dict[tp_nm.lfc_mw_diff] == None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_mw_diff} is not avialiable from topic: '{tp_nm.lfc_mw_diff}'. Setting to zero.")
        current_lfc_mw_diff = 0
    else:
        current_lfc_mw_diff = round(topic_latest_message_value_dict[tp_nm.lfc_mw_diff][msg_val_nm.lfc_mw_diff],PARM.PRECISION_DECIMALS)

    if topic_latest_message_value_dict[tp_nm.lfc_pbr_response] == None:
        add_to_log(f"Warning: Value: {msg_val_nm.lfc_pbr_response} is not avialiable from topic: '{tp_nm.lfc_pbr_response}'. Setting to zero.")
        last_pbr_response = 0
    else:
        last_pbr_response = round(topic_latest_message_value_dict[tp_nm.lfc_pbr_response][msg_val_nm.lfc_pbr_response],PARM.PRECISION_DECIMALS)

    add_to_log(f"Current target is: {current_lfc_p_target}")
    add_to_log(f"MW diff is: {current_lfc_mw_diff}")
           
    # simulate PBR responce
    # if target was reached within a deadband, then do not ramp
    if abs(current_lfc_p_target) - (PARM.DEADBAND_PBR_SIMU) < abs(last_pbr_response) < abs(current_lfc_p_target) + (PARM.DEADBAND_PBR_SIMU):
        add_to_log(f"PBR did not ramp due to deadband.")
        response_pbr = last_pbr_response
    # if ramping down is needed
    elif last_pbr_response > current_lfc_p_target: 
        add_to_log(f"PBR is regulating down.")
        response_pbr = round(last_pbr_response - (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU),PARM.PRECISION_DECIMALS) 
    # if ramping up is needed
    elif last_pbr_response < current_lfc_p_target:
        add_to_log(f"PBR is regulating up.")
        response_pbr = round(last_pbr_response + (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU),PARM.PRECISION_DECIMALS) 

    # send current pbr repsonce to kafka topic
    try:
        producer_kafka.send(tp_nm.lfc_pbr_response, value={msg_val_nm.lfc_pbr_response : response_pbr})
    except Exception as e:
        add_to_log(f"Error: Sending message to Kafka failed with message '{e}'.")
        sys.exit(1)

    add_to_log(f"PBR Response is now: {response_pbr}")

    # send system responce (sum of mw diff and PBR response) to kafka topic
    response_system = round(current_lfc_mw_diff+response_pbr,PARM.PRECISION_DECIMALS)
    try:
        producer_kafka.send(tp_nm.lfc_p_dem, value={msg_val_nm.lfc_p_dem : response_system})
    except Exception as e:
        add_to_log(f"Error: Sending message to Kafka failed with message: '{e}'.")
        sys.exit(1)
    
    add_to_log(f"System response: {response_system} was send as new LFC demand")

    add_to_log(f"|-------------------------------------------------|")
    sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
