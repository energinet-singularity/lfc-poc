# Import dependencies
from time import sleep, time

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import (add_to_log)

# Import parameters, Kafka topic names and message value names
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

    add_to_log("Info: Initilizing simulation of LFC P_demand response....")

    # Create lists of topic names which are consumed and produced
    topics_produced_list = [tp_nm.lfc_p_dem, tp_nm.lfc_pbr_response]
    topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_mw_diff, tp_nm.lfc_pbr_response]

    # inint kafka
    consumer_gp_nm = "lfc_demand_response_simu"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    add_to_log("Info: Simulating LFC P_demand response....")

    """
    Looping to simulate reponse of elctrical grid. Simulation is done by:
    1. Simulating response of PBR to at given setpoint and PBR parameters. The setpoint is current p_target.
    2. Calculate sum of simulated PBR response and MW_diff (difference between consumption and production in grid).
    3. Send calculated sum as p_demand to Kafka, as it will correspond to actual demand in the grid.
    """
    while True:
        # Save start time for loop
        time_loop_start = time()

        # check if topics which are both produced and consumed are empty, else init
        empty_consumed_and_produced_topics = kafka_obj.list_empty_consumed_and_produced_topics()
        for topic in empty_consumed_and_produced_topics:
            if topic == tp_nm.lfc_pbr_response:
                add_to_log(f"Info: Topic {topic} was empty. Initialised with default value.")
                kafka_obj.produce_message(topic_name=tp_nm.lfc_pbr_response,
                                          msg_value={msg_val_nm.lfc_pbr_response: 0})

        # check if consumed only data is availiable and wait if not, else do it
        empty_consumed_only_topics = kafka_obj.list_empty_consumed_only_topics()
        if empty_consumed_only_topics:
            sleep(1)
            add_to_log(f"Warning: The consumed only topics: {empty_consumed_only_topics} are empty. Waiting for input data.")
        else:
            # get latest messages from consumed topics
            msg_val_dict = kafka_obj.get_latest_msg_from_consumed_topics_to_dict()
            # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

            # getting values
            # TODO: Rounding?
            current_lfc_p_target = msg_val_dict[tp_nm.lfc_p_target][msg_val_nm.lfc_p_target]
            current_lfc_mw_diff = msg_val_dict[tp_nm.lfc_mw_diff][msg_val_nm.lfc_mw_diff]
            last_pbr_response = msg_val_dict[tp_nm.lfc_pbr_response][msg_val_nm.lfc_pbr_response]

            # Calculate simulated PBR responce
            response_pbr = calc_simulated_pbr_response(p_target=current_lfc_p_target,
                                                       last_pbr_response=last_pbr_response)
            add_to_log(f"Info: PBR response is: {response_pbr}")
            add_to_log(f"Info: MW_diff is: {current_lfc_mw_diff}")

            # Send current pbr repsonce to kafka topic
            kafka_obj.produce_message(topic_name=tp_nm.lfc_pbr_response,
                                      msg_value={msg_val_nm.lfc_pbr_response: response_pbr})

            # Send simulated electrical grid responce (sum of mw diff and PBR response) to kafka topic
            response_system = round(current_lfc_mw_diff+response_pbr, PARM.PRECISION_DECIMALS)
            kafka_obj.produce_message(topic_name=tp_nm.lfc_p_dem,
                                      msg_value={msg_val_nm.lfc_p_dem: response_system})
            add_to_log(f"Info: System response: {response_system} was send as LFC p_demand")

            # sleep
            sleep(PARM.REFRESH_RATE_S_LFC_DEM_SIMU)
