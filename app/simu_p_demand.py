from time import sleep, time
import sys
from functions_kafka import (set_kafka_brooker_from_env, init_producer, init_consumer, init_topic_partitions,
                             subscribe_topics, produce_message,
                             get_latest_topic_messages_to_dict_poll_based, get_msg_val_from_dict,
                             list_unavbl_topics, list_empty_topics)
from functions_lfc import (add_to_log, simulate_pbr_response, print_lfc_logo)

# importing settings
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC P_demand response simulation initialization..")

    # Kafka brooker set from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # list of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_dem, tp_nm.lfc_pbr_response]
    topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_mw_diff, tp_nm.lfc_pbr_response]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # init dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Kafka producer init
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Kafka consumer init
    consumer_gp_nm = "lfc_demand_response_simu"
    consumer_kafka = init_consumer(bootstrap_servers=kafka_brooker,
                                   group_id=consumer_gp_nm,
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=False)

    # subscribe to consumed topics
    subscribe_topics(consumer=consumer_kafka, topic_list=topics_consumed_list)

    # dummy poll needed for partiontions to be assigned
    init_topic_partitions(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

    # Verify if all needed kafka topics exist
    if list_unavbl_topics(consumer=consumer_kafka, topic_list=topics_list, print_err=True):
        sys.exit(1)

    add_to_log("Info: Simulating LFC P_demand response....")
    add_to_log("|-------------------------------------------------|")

    while True:
        # saving start time for loop
        time_loop_start = time()

        # Getting latest value for each comnsumed topic
        topic_latest_message_value_dict = get_latest_topic_messages_to_dict_poll_based(consumer=consumer_kafka,
                                                                                       topic_list=topics_consumed_list,
                                                                                       timeout_ms=PARM.TIMEOUT_MS_POLL)
        # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # report warnings on empty topics
        list_empty_topics(topic_latest_message_value_dict=topic_latest_message_value_dict, print_warn=True)

        # init empty list, used for adding topics which are consumed, but has no messages
        topics_consumed_unavbl_msg = []

        # extract value (topic/value specific) and round to decimals defined by precision variable
        # if message value ts not availiable handle it either by:
        # - using a default value (if the consumed topic is also produced by this function)
        # - adding to list and report error later (if consumed only).
        # TODO make smarter. Nested dict for holding data values instead of variables?

        # current lfc p target (consumed only)
        current_lfc_p_target = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                     tp_nm=tp_nm.lfc_p_target,
                                                     msg_val_nm=msg_val_nm.lfc_p_target,
                                                     default_val=None)
        if current_lfc_p_target is None:
            topics_consumed_unavbl_msg.append(tp_nm.lfc_p_target)
        else:
            current_lfc_p_target = round(current_lfc_p_target, PARM.PRECISION_DECIMALS)

        # current mw diff (consumed only)
        current_lfc_mw_diff = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                    tp_nm=tp_nm.lfc_mw_diff,
                                                    msg_val_nm=msg_val_nm.lfc_mw_diff,
                                                    default_val=None)
        if current_lfc_mw_diff is None:
            topics_consumed_unavbl_msg.append(tp_nm.lfc_p_target)
        else:
            current_lfc_mw_diff = round(current_lfc_mw_diff, PARM.PRECISION_DECIMALS)

        # last_pbr_response (consumed and produced, default val: 0)
        last_pbr_response = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                  tp_nm=tp_nm.lfc_pbr_response,
                                                  msg_val_nm=msg_val_nm.lfc_pbr_response,
                                                  default_val=0)
        last_pbr_response = round(last_pbr_response, PARM.PRECISION_DECIMALS)

        # report missing needed data and exit
        if topics_consumed_unavbl_msg:
            add_to_log(f"Error: Topic(s) '{topics_consumed_unavbl_msg}' does not contain any messages, but is needed.")
            sys.exit(1)

        add_to_log(f"Info: LFC P_target is: {current_lfc_p_target}")

        # simulate PBR responce
        response_pbr = simulate_pbr_response(p_target=current_lfc_p_target,
                                             last_pbr_response=last_pbr_response)
        add_to_log(f"Info: PBR response is: {response_pbr}")
        add_to_log(f"Info: MW_diff is: {current_lfc_mw_diff}")

        # send current pbr repsonce to kafka topic
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_pbr_response,
                        value={msg_val_nm.lfc_pbr_response: response_pbr})

        # send system responce (sum of mw diff and PBR response) to kafka topic
        response_system = round(current_lfc_mw_diff+response_pbr, PARM.PRECISION_DECIMALS)
        produce_message(producer=producer_kafka, topic_name=tp_nm.lfc_p_dem,
                        value={msg_val_nm.lfc_p_dem: response_system})
        add_to_log(f"Info: System response: {response_system} was send as new LFC P_demand")

        # add_to_log(f"Debug: Loop took: {round(time()-time_loop_start,3)} secounds.")
        add_to_log("|-------------------------------------------------|")

        sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
