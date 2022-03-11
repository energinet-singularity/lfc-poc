# Import dependencies
from time import sleep, time
import logging

# Import functions
from singukafka import KafkaHelper

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

# pbr responce simu settings (constants)
REFRESH_RATE_S_LFC_DEM_SIMU = 1
PBR_RAMP_MWM = 50
PBR_RAMP_MWS = PBR_RAMP_MWM/60
DEADBAND_PBR_SIMU = PBR_RAMP_MWS*REFRESH_RATE_S_LFC_DEM_SIMU
PRECISION_DECIMALS = 2

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARN)


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
    if abs(p_target) - (DEADBAND_PBR_SIMU/2) <= abs(last_pbr_response) <= abs(p_target) + (DEADBAND_PBR_SIMU/2):
        log.info("PBR did not ramp due to deadband.")
        response_pbr = last_pbr_response
    # if ramping down is needed
    elif last_pbr_response > p_target:
        log.info("Info: PBR is regulating down.")
        response_pbr = round(last_pbr_response - (PBR_RAMP_MWS*REFRESH_RATE_S_LFC_DEM_SIMU), PRECISION_DECIMALS)
    # if ramping up is needed
    elif last_pbr_response < p_target:
        log.info("Info: PBR is regulating up.")
        response_pbr = round(last_pbr_response + (PBR_RAMP_MWS*REFRESH_RATE_S_LFC_DEM_SIMU), PRECISION_DECIMALS)

    return response_pbr


if __name__ == "__main__":

    log.info("Initilizing simulation of LFC P_demand response....")

    # Create lists of topic names which are consumed and produced
    topics_produced_list = [tp_nm.lfc_p_dem, tp_nm.lfc_pbr_response]
    topics_consumed_list = [tp_nm.lfc_mw_diff, tp_nm.lfc_pbr_response, tp_nm.lfc_bsp_activated]
    # tp_nm.lfc_p_target

    # init default val
    current_lfc_p_target = 0
    current_lfc_mw_diff = 0
    last_pbr_response = 0

    # inint kafka
    consumer_gp_nm = "lfc_demand_response_simu"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list)

    log.info("Simulating LFC P_demand response....")

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
                log.info(f"Topic {topic} was empty. Initialised with default value.")
                kafka_obj.produce_message(topic_name=tp_nm.lfc_pbr_response,
                                          msg_key=msg_val_nm.lfc_pbr_response,
                                          msg_value={msg_val_nm.lfc_pbr_response: 0})

        # get latest messages from consumed topics
        # TODO function with possibility of setting default val an suppliyin latest fetched val?
        dataframe = kafka_obj.get_kafka_messages_to_pandas_dataframe()

        value_bsp = kafka_obj.extract_value_from_kafka_dataframe(dataframe=dataframe, msg_key_filter=msg_val_nm.lfc_bsp_activated)
        if value_bsp is not None:
            current_lfc_p_target = round(sum(item['setpoint'] for item in value_bsp[msg_val_nm.lfc_bsp_activated]), 2)

        value_lfc_mw_diff = kafka_obj.extract_value_from_kafka_dataframe(dataframe=dataframe, msg_key_filter=msg_val_nm.lfc_mw_diff)
        if value_lfc_mw_diff is not None:
            current_lfc_mw_diff = value_lfc_mw_diff[msg_val_nm.lfc_mw_diff]

        value_last_pbr = kafka_obj.extract_value_from_kafka_dataframe(dataframe=dataframe, msg_key_filter=msg_val_nm.lfc_pbr_response)
        if value_last_pbr is not None:
            last_pbr_response = value_last_pbr[msg_val_nm.lfc_pbr_response]

        # Calculate simulated PBR responce
        response_pbr = calc_simulated_pbr_response(p_target=current_lfc_p_target,
                                                   last_pbr_response=last_pbr_response)
        log.info(f"PBR response is: {response_pbr}")
        log.info(f"MW_diff is: {current_lfc_mw_diff}")

        # Send current pbr repsonce to kafka topic
        kafka_obj.produce_message(topic_name=tp_nm.lfc_pbr_response,
                                  msg_key=msg_val_nm.lfc_pbr_response,
                                  msg_value={msg_val_nm.lfc_pbr_response: response_pbr})

        # Send simulated electrical grid responce (sum of mw diff and PBR response) to kafka topic
        response_system = round(current_lfc_mw_diff+response_pbr, PRECISION_DECIMALS)
        kafka_obj.produce_message(topic_name=tp_nm.lfc_p_dem,
                                  msg_key=msg_val_nm.lfc_p_dem,
                                  msg_value={msg_val_nm.lfc_p_dem: response_system})
        log.info(f"System response: {response_system} was send as LFC p_demand.")
        loop_time = round(time()-time_loop_start, 3)
        log.info(f"Loop took {loop_time} secounds.")

        # sleep
        if loop_time < 1:
            sleep(1-loop_time)
