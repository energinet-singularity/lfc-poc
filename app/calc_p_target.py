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
    add_to_log("Info: LFC P_Target calculcation initializing.")

    # Kafka brooker set from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # list of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_target, tp_nm.lfc_p_target_state]
    topics_consumed_list = [tp_nm.lfc_p_input, tp_nm.lfc_p_target_state]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # init dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Kafka producer init
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Kafka consumer init
    consumer_gp_nm = "lfc_target_calc"
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

    # LFC target value (always zero)
    setpoint_lfc_p_input = 0

    add_to_log("LFC P Target (PID controller)  running....")
    add_to_log("|-----------------------------------------|")

    while True:

        error_found = False

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
        current_lfc_p_input = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                    tp_nm=tp_nm.lfc_p_input,
                                                    msg_val_nm=msg_val_nm.lfc_p_input,
                                                    default_val=None)
        if current_lfc_p_input is None:
            topics_consumed_unavbl_msg.append(tp_nm.lfc_p_input)
        else:
            current_lfc_p_input = round(current_lfc_p_input, PARM.PRECISION_DECIMALS)

        # last_error (consumed and produced, default val: 0)
        last_error = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                           tp_nm=tp_nm.lfc_p_target_state,
                                           msg_val_nm=msg_val_nm.lfc_p_target_error,
                                           default_val=0)
        last_error = round(last_error, PARM.PRECISION_DECIMALS)

        # last_error_sum (consumed and produced, default val: 0)
        last_error_sum = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                               tp_nm=tp_nm.lfc_p_target_state,
                                               msg_val_nm=msg_val_nm.lfc_p_target_error_sum,
                                               default_val=0)
        last_error_sum = round(last_error_sum, PARM.PRECISION_DECIMALS)

        # print LFC input
        add_to_log(f"Info: LFC P_input is: {current_lfc_p_input}")

        # calculate LFC target
        add_to_log("Info: Calculating LFC P_target...")

        # save cycle start time
        time_current_cycle_start = datetime.now()

        """
        # if cycle time too long
        if ((time_current_cycle_start - time_last_cycle_start).total_seconds()) > 4:
            add_to_log("LFC cycle time was not respected.")
        else:
            # wait until
            while ((datetime.now() - time_last_cycle_start).total_seconds()) < 4:
                sleep(0.1)
        """

        # PID controller (if error within deadband values set error = 0)
        # TODO make as function, possible cold start flag also
        error = setpoint_lfc_p_input - current_lfc_p_input

        if abs(error) < PARM.DEADBAND_LFC_ERROR:
            error = 0
            add_to_log(f"Info: LFC error is respecting deadband: {PARM.DEADBAND_LFC_ERROR}")

        error_sum = last_error_sum + (error * PARM.CYCLETIME_S_LFC_CONT)
        error_diff = (error - last_error) / PARM.CYCLETIME_S_LFC_CONT

        lfc_p_target = round(PARM.KP * error + PARM.KI * error_sum + PARM.KD * error_diff, PARM.PRECISION_DECIMALS)

        # values for next cycle
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_p_target_state,
                        value={'Timestamp': str(time_current_cycle_start),
                               msg_val_nm.lfc_p_target: lfc_p_target,
                               msg_val_nm.lfc_p_target_error: error,
                               msg_val_nm.lfc_p_target_error_sum: error_sum,
                               msg_val_nm.lfc_p_target_error_diff: error_diff}
                        )
        # send LFC target value to topic
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_p_target,
                        value={msg_val_nm.lfc_p_target: lfc_p_target})

        add_to_log(f"Info: LFC P_target is: {lfc_p_target}")

        add_to_log("|-----------------------------------------|")
        sleep(PARM.CYCLETIME_S_LFC_CONT)
