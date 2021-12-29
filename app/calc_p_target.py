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


def lfc_pid_controller(meas: float, last_error: float, last_error_sum: float, cycletime_s: float):
    """

    """
    error = PARM.SETPOINT_LFC_P_INPUT - meas

    if abs(error) < PARM.DEADBAND_LFC_ERROR:
        error = 0
        add_to_log(f"Info: LFC error is respecting deadband: {PARM.DEADBAND_LFC_ERROR}")

    error_sum = last_error_sum + (error * cycletime_s)
    error_diff = (error - last_error) / cycletime_s

    target = round(PARM.KP * error + PARM.KI * error_sum + PARM.KD * error_diff, PARM.PRECISION_DECIMALS)

    return target, error, error_sum, error_diff


if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC p_target calculation initializing..")

    # Set Kafka brooker from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # Create lists of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_target, tp_nm.lfc_p_target_state]
    topics_consumed_list = [tp_nm.lfc_p_input, tp_nm.lfc_p_target_state]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # Create dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Initialize Kafka producer
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Initialize Kafka consumer
    consumer_gp_nm = "lfc_target_calc"
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

    add_to_log("Calculating LFC p_target by use of PID-controller..")
    add_to_log("|-----------------------------------------|")

    """
    TODO describe
    """
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
        # - current lfc p corr (consumed only, default val: 0)
        current_lfc_p_input = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                    tp_nm=tp_nm.lfc_p_input,
                                                    msg_val_nm=msg_val_nm.lfc_p_input,
                                                    default_val=None,
                                                    precision=PARM.PRECISION_DECIMALS)

        # - last_error (consumed and produced, default val: 0)
        last_error = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                           tp_nm=tp_nm.lfc_p_target_state,
                                           msg_val_nm=msg_val_nm.lfc_p_target_error,
                                           default_val=0,
                                           precision=PARM.PRECISION_DECIMALS)

        # - last_error_sum (consumed and produced, default val: 0)
        last_error_sum = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                               tp_nm=tp_nm.lfc_p_target_state,
                                               msg_val_nm=msg_val_nm.lfc_p_target_error_sum,
                                               default_val=0,
                                               precision=PARM.PRECISION_DECIMALS)

        # TODO - time_last_cycle_start (consumed and produce, default val?)

        add_to_log(f"Info: LFC p_input is: {current_lfc_p_input}. Calculating p_target..")

        """
        TODO calc cycle time baseret på seneste kørsel, hvad hvis meget lang (2xcycle time) = restart?
        TODO sleep indtil 4 sekunder gået?
        # if cycle time too long
        if ((time_current_cycle_start - time_last_cycle_start).total_seconds()) > 4:
            add_to_log("LFC cycle time was not respected.")
        else:
            # wait until
            while ((datetime.now() - time_last_cycle_start).total_seconds()) < 4:
                sleep(0.1)
        """
        # Save cycle start time
        time_current_cycle_start = datetime.now()

        # Run PID controller
        lfc_p_target, error, error_sum, error_diff = lfc_pid_controller(meas=current_lfc_p_input,
                                                                        last_error=last_error,
                                                                        last_error_sum=last_error_sum,
                                                                        cycletime_s=PARM.CYCLETIME_S_LFC)
        add_to_log(f"Info: LFC P_target is: {lfc_p_target}")

        # Send state of calculation to Kakfa foir usage in enxt cycle
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_p_target_state,
                        value={'Timestamp': str(time_current_cycle_start),
                               msg_val_nm.lfc_p_target: lfc_p_target,
                               msg_val_nm.lfc_p_target_error: error,
                               msg_val_nm.lfc_p_target_error_sum: error_sum,
                               msg_val_nm.lfc_p_target_error_diff: error_diff}
                        )
        # Send LFC p_target value to topic
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_p_target,
                        value={msg_val_nm.lfc_p_target: lfc_p_target})

        add_to_log("|-----------------------------------------|")
        sleep(PARM.CYCLETIME_S_LFC)
