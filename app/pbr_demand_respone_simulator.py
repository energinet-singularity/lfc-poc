from time import sleep, time
import sys
from functions_kafka import (set_kafka_brooker_from_env, init_producer, init_consumer, init_topic_partitions,
                             subscribe_topics, produce_message, get_latest_topic_messages_to_dict_poll_based,
                             list_unavbl_topics, list_empty_topics)
from functions_lfc import (add_to_log, simulate_pbr_response, print_lfc_logo)

# importing settings
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC demand response simulation initialization..")

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

    # Kafka consumer
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
    unavbl_topcis = list_unavbl_topics(consumer=consumer_kafka, topic_list=topics_list)
    if unavbl_topcis:
        add_to_log(f"Error: The Topic(s): {unavbl_topcis} does not exist.")
        sys.exit(1)

    add_to_log("Info: Simulating LFC Demand response....")
    add_to_log("|-------------------------------------------------|")

    while True:

        # saving staty time for loop
        time_loop_start = time()

        # Getting latest value for each comnsumed topic
        topic_latest_message_value_dict = get_latest_topic_messages_to_dict_poll_based(consumer=consumer_kafka,
                                                                                       topic_list=topics_consumed_list,
                                                                                       timeout_ms=PARM.TIMEOUT_MS_POLL)
        add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # report warnings on empty topics
        topics_empty = list_empty_topics(topic_latest_message_value_dict=topic_latest_message_value_dict)
        if topics_empty:
            add_to_log(f"Warning: No data was availiable on consumed Kafka Topic(s): {topics_empty}.")

        # extract value, topic specific, and round to decimals defined by precision varialbe
        # TODO simplify/make smarter (Avro schema?)
        if topic_latest_message_value_dict[tp_nm.lfc_p_target] is None:
            add_to_log(f"Warning: Value: {msg_val_nm.lfc_p_target} is not avialiable from topic: " +
                       "'{tp_nm.lfc_p_target}'. Setting to zero.")
            current_lfc_p_target = 0
        else:
            # TODO build safety again wrongly formattet message
            current_lfc_p_target = round(topic_latest_message_value_dict[tp_nm.lfc_p_target][msg_val_nm.lfc_p_target],
                                         PARM.PRECISION_DECIMALS)

        if topic_latest_message_value_dict[tp_nm.lfc_mw_diff] is None:
            add_to_log(f"Warning: Value: {msg_val_nm.lfc_mw_diff}" +
                       " is not avialiable from topic: '{tp_nm.lfc_mw_diff}'. Setting to zero.")
            current_lfc_mw_diff = 0
        else:
            current_lfc_mw_diff = round(topic_latest_message_value_dict[tp_nm.lfc_mw_diff][msg_val_nm.lfc_mw_diff],
                                        PARM.PRECISION_DECIMALS)

        if topic_latest_message_value_dict[tp_nm.lfc_pbr_response] is None:
            add_to_log(f"Warning: Value: {msg_val_nm.lfc_pbr_response}" +
                       " is not avialiable from topic: '{tp_nm.lfc_pbr_response}'. Setting to zero.")
            last_pbr_response = 0
        else:
            last_pbr_response = round(topic_latest_message_value_dict[tp_nm.lfc_pbr_response][msg_val_nm.lfc_pbr_response],
                                      PARM.PRECISION_DECIMALS)

        add_to_log(f"Info: Current target is: {current_lfc_p_target}")
        add_to_log(f"Info: MW diff is: {current_lfc_mw_diff}")

        # simulate PBR responce
        response_pbr = simulate_pbr_response(p_target=current_lfc_p_target,
                                             last_pbr_response=last_pbr_response)

        # send current pbr repsonce to kafka topic
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_pbr_response,
                        value={msg_val_nm.lfc_pbr_response: response_pbr})

        # send system responce (sum of mw diff and PBR response) to kafka topic
        response_system = round(current_lfc_mw_diff+response_pbr, PARM.PRECISION_DECIMALS)
        produce_message(producer=producer_kafka, topic_name=tp_nm.lfc_p_dem,
                        value={msg_val_nm.lfc_p_dem: response_system})
        add_to_log(f"Info: System response: {response_system} was send as new LFC demand")

        add_to_log(f"Debug: Loop took: {round(time()-time_loop_start,3)} secounds.")
        add_to_log("|-------------------------------------------------|")

        sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
