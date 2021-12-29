# Import dependencies
from time import sleep, time
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


def calc_simulated_pbr_response(p_target: float, last_pbr_response: float):
    """ PBR response simulation.

    Simulates the response of PBR. PBR responce should follow given p_target.
    If it is not following it will either ramp up or ramp down according to defined ramping parameters.

    Argurments:
        p_target (float): MW target value.
        last_pbr_repsponse (float): last response of simulated PBR response.
    Returns:
        response_pbr (float): simulated MW reponse of PBR.

    """
    # When within deadband, do not ramp
    if abs(p_target) - (PARM.DEADBAND_PBR_SIMU) < abs(last_pbr_response) < abs(p_target) + (PARM.DEADBAND_PBR_SIMU):
        add_to_log("Info: PBR did not ramp due to deadband.")
        response_pbr = last_pbr_response
    # if ramping down is needed
    elif last_pbr_response > p_target:
        add_to_log("Info: PBR is regulating down.")
        response_pbr = round(last_pbr_response - (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU), PARM.PRECISION_DECIMALS)
    # if ramping up is needed
    elif last_pbr_response < p_target:
        add_to_log("Info: PBR is regulating up.")
        response_pbr = round(last_pbr_response + (PARM.PBR_RAMP_MWS*PARM.REFRESH_RATE_S_LFC_DEM_SIMU), PARM.PRECISION_DECIMALS)

    return response_pbr


if __name__ == "__main__":
    print_lfc_logo()
    add_to_log("Info: LFC p_demand response simulation initializing..")

    # Set Kafka brooker from environment variables
    kafka_brooker = set_kafka_brooker_from_env()

    # Create lists of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_dem, tp_nm.lfc_pbr_response]
    topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_mw_diff, tp_nm.lfc_pbr_response]
    topics_list = list(set(topics_produced_list + topics_consumed_list))

    # Create dictionary for holding latest message value for each consumed topic
    topic_latest_message_value_dict = {}

    # Initialize Kafka producer
    producer_kafka = init_producer(bootstrap_servers=kafka_brooker)

    # Initialize Kafka consumer
    consumer_gp_nm = "lfc_demand_response_simu"
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

    add_to_log("Info: Simulating LFC P_demand response....")
    add_to_log("|-------------------------------------------------|")

    """
    Looping to simulate reponse of elctrical grid. Simulation is done by:
    1. Simulating response of PBR to at given setpoint and PBR parameters. The setpoint is current p_target.
    2. Calculate sum of simulated PBR response and MW_diff (difference between consumption and production in grid).
    3. Send calculated sum as p_demand to Kafka, as it will correspond to actual demand in the grid.
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
        # TODO make smarter. Nested dict for holding data values instead of variables?
        # - current LFC p_target (consumed only, default val: 0)
        current_lfc_p_target = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                     tp_nm=tp_nm.lfc_p_target,
                                                     msg_val_nm=msg_val_nm.lfc_p_target,
                                                     default_val=0,
                                                     precision=PARM.PRECISION_DECIMALS)

        # - current mw diff (consumed only, default val: 0)
        current_lfc_mw_diff = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                    tp_nm=tp_nm.lfc_mw_diff,
                                                    msg_val_nm=msg_val_nm.lfc_mw_diff,
                                                    default_val=0,
                                                    precision=PARM.PRECISION_DECIMALS)

        # - last pbr_response (consumed and produced, default val: 0)
        last_pbr_response = get_msg_val_from_dict(msg_val_dict=topic_latest_message_value_dict,
                                                  tp_nm=tp_nm.lfc_pbr_response,
                                                  msg_val_nm=msg_val_nm.lfc_pbr_response,
                                                  default_val=0,
                                                  precision=PARM.PRECISION_DECIMALS)

        # Calculate simulated PBR responce
        response_pbr = calc_simulated_pbr_response(p_target=current_lfc_p_target,
                                                   last_pbr_response=last_pbr_response)
        add_to_log(f"Info: PBR response is: {response_pbr}")
        add_to_log(f"Info: MW_diff is: {current_lfc_mw_diff}")

        # Send current pbr repsonce to kafka topic
        produce_message(producer=producer_kafka,
                        topic_name=tp_nm.lfc_pbr_response,
                        value={msg_val_nm.lfc_pbr_response: response_pbr})

        # Send simulated electrical grid responce (sum of mw diff and PBR response) to kafka topic
        response_system = round(current_lfc_mw_diff+response_pbr, PARM.PRECISION_DECIMALS)
        produce_message(producer=producer_kafka, topic_name=tp_nm.lfc_p_dem,
                        value={msg_val_nm.lfc_p_dem: response_system})
        add_to_log(f"Info: System response: {response_system} was send as new LFC p_demand")

        # add_to_log(f"Debug: Loop took: {round(time()-time_loop_start,3)} secounds.")
        add_to_log("|-------------------------------------------------|")

        # sleep
        sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
