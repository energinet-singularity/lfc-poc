from time import sleep, time
from datetime import datetime
import sys
from functions_kafka import (set_kafka_brooker_from_env, init_producer, init_consumer, init_topic_partitions,
                             subscribe_topics, produce_message,
                             get_latest_topic_messages_to_dict_poll_based, get_msg_val_from_dict,
                             list_unavbl_topics, list_empty_topics)
from functions_lfc import (add_to_log, print_lfc_logo)

# importing settings
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC_P_Input calculation initialization..")

    # Kafka brooker set from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # list of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_input_calc_state, tp_nm.lfc_p_input]
    topics_consumed_list = [tp_nm.lfc_p_corr, tp_nm.lfc_p_dem, tp_nm.lfc_p_input_calc_state]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # init dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Kafka producer init
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Kafka consumer init
    consumer_gp_nm = "lfc_input_calc"
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

    # init used to determine first loop of function
    is_first_loop = True

    add_to_log("Info: Calcualting LFC input when LFC demand or LFC correction changes....")
    add_to_log("|-----------------------------------------|")

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

        # current lfc p corr (consumed only)
        current_lfc_p_corr = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                   tp_nm=tp_nm.lfc_p_corr,
                                                   msg_val_nm=msg_val_nm.lfc_p_corr,
                                                   default_val=None)
        if current_lfc_p_corr is None:
            topics_consumed_unavbl_msg.append(tp_nm.lfc_p_corr)
        else:
            current_lfc_p_corr = round(current_lfc_p_corr, PARM.PRECISION_DECIMALS)

        # current lfc p dem (consumed only)
        current_lfc_p_dem = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                  tp_nm=tp_nm.lfc_p_dem,
                                                  msg_val_nm=msg_val_nm.lfc_p_dem,
                                                  default_val=None)
        if current_lfc_p_dem is None:
            topics_consumed_unavbl_msg.append(tp_nm.lfc_p_dem)
        else:
            current_lfc_p_dem = round(current_lfc_p_dem, PARM.PRECISION_DECIMALS)

        # last_lfc_p_corr (consumed and produced, default val: 0)
        last_lfc_p_corr = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                tp_nm=tp_nm.lfc_p_input_calc_state,
                                                msg_val_nm=msg_val_nm.last_lfc_p_corr,
                                                default_val=0)
        last_lfc_p_corr = round(last_lfc_p_corr, PARM.PRECISION_DECIMALS)

        # last_lfc_p_dem (consumed and produced, default val: 0)
        last_lfc_p_dem = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                               tp_nm=tp_nm.lfc_p_input_calc_state,
                                               msg_val_nm=msg_val_nm.last_lfc_p_dem,
                                               default_val=0)
        last_lfc_p_dem = round(last_lfc_p_dem, PARM.PRECISION_DECIMALS)

        # report missing needed data and exit
        if topics_consumed_unavbl_msg:
            add_to_log(f"Error: Topic(s) '{topics_consumed_unavbl_msg}' does not contain any messages, but is needed.")
            sys.exit(1)

        # calculate new lfc input if: lfc demand changed, lfc correction changed or it is first loop af function
        if last_lfc_p_dem != current_lfc_p_dem or last_lfc_p_corr != current_lfc_p_corr or is_first_loop:

            # log if demand remained the same
            if last_lfc_p_dem == current_lfc_p_dem:
                add_to_log(f"Info: LFC Demand is still: {current_lfc_p_dem}")
            else:
                add_to_log(f"Info: LFC Demand changed to: {current_lfc_p_dem} from: {last_lfc_p_dem}")

            # log if Correction remained the same
            if last_lfc_p_corr == current_lfc_p_corr:
                add_to_log(f"Info: LFC Correction is still: {current_lfc_p_corr}")
            else:
                add_to_log(f"Info: LFC Correction changed to: {current_lfc_p_corr} from: {last_lfc_p_corr}")

            # save cycle start time
            time_current_calc_start = datetime.now()

            # calculate and print lfc input
            lfc_p_input = round(current_lfc_p_dem + current_lfc_p_corr, PARM.PRECISION_DECIMALS)

            add_to_log(f"Info: LFC Input is now: {lfc_p_input}")
            add_to_log("|-----------------------------------------|")

            # send lfc input to kafka
            produce_message(producer=producer_kafka,
                            topic_name=tp_nm.lfc_p_input,
                            value={msg_val_nm.lfc_p_input: lfc_p_input})

            # send new state as message to kafka
            produce_message(producer=producer_kafka,
                            topic_name=tp_nm.lfc_p_input_calc_state,
                            value={'Timestamp': str(time_current_calc_start),
                                   msg_val_nm.last_lfc_p_dem: current_lfc_p_dem,
                                   msg_val_nm.last_lfc_p_corr: current_lfc_p_corr,
                                   msg_val_nm.lfc_p_input: lfc_p_input}
                            )

        is_first_loop = False

        sleep(PARM.REFRESH_RATE_MS_LFC_INPUT/1000)
