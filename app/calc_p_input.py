# Import dependencies
from time import sleep, time
from datetime import datetime
import sys

# Importing function
from functions_kafka import (set_kafka_brooker_from_env, init_producer, init_consumer, init_topic_partitions,
                             subscribe_topics, produce_message,
                             get_latest_topic_messages_to_dict_poll_based, get_msg_val_from_dict,
                             list_unavbl_topics, list_empty_topics)
from functions_lfc import (add_to_log, print_lfc_logo)

# Importing parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC p_input calculation initializing..")

    # Set Kafka brooker from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # Create lists of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_input_calc_state, tp_nm.lfc_p_input]
    topics_consumed_list = [tp_nm.lfc_p_corr, tp_nm.lfc_p_dem, tp_nm.lfc_p_input_calc_state]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # Create dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Initialize Kafka producer
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Initialize Kafka consumer
    consumer_gp_nm = "lfc_input_calc"
    consumer_kafka = init_consumer(bootstrap_servers=kafka_brooker,
                                   group_id=consumer_gp_nm,
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=False)

    # Subscribe to consumed topics
    subscribe_topics(consumer=consumer_kafka, topic_list=topics_consumed_list)

    # Dummy poll needed to force partitions assignment
    init_topic_partitions(consumer=consumer_kafka, topic_list=topics_consumed_list, timeout_ms=PARM.TIMEOUT_MS_POLL)

    # Verify if all needed kafka topics exist
    if list_unavbl_topics(consumer=consumer_kafka, topic_list=topics_list, print_err=True):
        sys.exit(1)

    # Set booleam value used to determine if first loop of function
    # TODO make this via state instead?
    is_first_loop = True

    add_to_log("Info: Calculating LFC p_input when p_demand or p_correction changes....")
    add_to_log("|-----------------------------------------|")

    # loop to calculate p_input (sum of p_demand and p_correction) and send to kafka, if p_demand and/or p_correction changed
    while True:
        # Save start time for loop
        time_loop_start = time()

        # Get latest value for each comnsumed topic
        topic_latest_message_value_dict = get_latest_topic_messages_to_dict_poll_based(consumer=consumer_kafka,
                                                                                       topic_list=topics_consumed_list,
                                                                                       timeout_ms=PARM.TIMEOUT_MS_POLL)
        # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # Report warnings on empty topics
        list_empty_topics(topic_latest_message_value_dict=topic_latest_message_value_dict, print_warn=True)

        # Extract values (topic/value specific) and round to decimals defined by precision variable.
        # If message value is not availiable from topic set value to 0.
        # - current LFC p_correction (consumed only, default val: 0)
        current_lfc_p_corr = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                   tp_nm=tp_nm.lfc_p_corr,
                                                   msg_val_nm=msg_val_nm.lfc_p_corr,
                                                   default_val=0,
                                                   precision=PARM.PRECISION_DECIMALS)

        # - current LFC p_demand (consumed only, default val: 0)
        current_lfc_p_dem = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                  tp_nm=tp_nm.lfc_p_dem,
                                                  msg_val_nm=msg_val_nm.lfc_p_dem,
                                                  default_val=0,
                                                  precision=PARM.PRECISION_DECIMALS)

        # - last LFC p_correction (consumed and produced, default val: 0)
        last_lfc_p_corr = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                tp_nm=tp_nm.lfc_p_input_calc_state,
                                                msg_val_nm=msg_val_nm.last_lfc_p_corr,
                                                default_val=0,
                                                precision=PARM.PRECISION_DECIMALS)

        # - last LFC p_demand (consumed and produced, default val: 0)
        last_lfc_p_dem = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                               tp_nm=tp_nm.lfc_p_input_calc_state,
                                               msg_val_nm=msg_val_nm.last_lfc_p_dem,
                                               default_val=0,
                                               precision=PARM.PRECISION_DECIMALS)

        # Calculate new LFC input and send to Kafka if:
        # LFC demand changed, LFC correction changed or it is first loop af function
        if last_lfc_p_dem != current_lfc_p_dem or last_lfc_p_corr != current_lfc_p_corr or is_first_loop:

            # Log if demand changed or remained the same
            if last_lfc_p_dem == current_lfc_p_dem:
                add_to_log(f"Info: LFC p_demand is still: {current_lfc_p_dem}")
            else:
                add_to_log(f"Info: LFC p_demand changed to: {current_lfc_p_dem} from: {last_lfc_p_dem}")

            # Log if correction changed or remained the same
            if last_lfc_p_corr == current_lfc_p_corr:
                add_to_log(f"Info: LFC p_correction is still: {current_lfc_p_corr}")
            else:
                add_to_log(f"Info: LFC p_correction changed to: {current_lfc_p_corr} from: {last_lfc_p_corr}")

            # Save cycle start time
            time_current_calc_start = datetime.now()

            # Calculate LFC p_input as a sum of p_demand and p_correction
            lfc_p_input = round(current_lfc_p_dem + current_lfc_p_corr, PARM.PRECISION_DECIMALS)

            add_to_log(f"Info: LFC p_input is now: {lfc_p_input}")
            add_to_log("|-----------------------------------------|")

            # Send LFC p_input to kafka
            produce_message(producer=producer_kafka,
                            topic_name=tp_nm.lfc_p_input,
                            value={msg_val_nm.lfc_p_input: lfc_p_input})

            # Send new state info for LFC p_input calculation to kafka
            produce_message(producer=producer_kafka,
                            topic_name=tp_nm.lfc_p_input_calc_state,
                            value={'Timestamp': str(time_current_calc_start),
                                   msg_val_nm.last_lfc_p_dem: current_lfc_p_dem,
                                   msg_val_nm.last_lfc_p_corr: current_lfc_p_corr,
                                   msg_val_nm.lfc_p_input: lfc_p_input}
                            )

        is_first_loop = False

        # Sleep before next loop
        sleep(PARM.REFRESH_RATE_MS_LFC_INPUT/1000)
