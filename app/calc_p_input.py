# Import dependencies
from time import sleep, time
from datetime import datetime
import logging

# Import functions
from singukafka import KafkaHelper

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm

# constants
PRECISION_DECIMALS = 2
REFRESH_RATE_CALC_P_INPUT = 1

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARNING)

if __name__ == "__main__":

    log.info("LFC p_input calculation initializing..")

    # Create lists of topic names which are consumed and produced
    topics_produced_list = [tp_nm.lfc_p_input_calc_state, tp_nm.lfc_p_input]
    topics_consumed_list = [tp_nm.lfc_p_corr, tp_nm.lfc_p_dem, tp_nm.lfc_p_input_calc_state]

    # init kafka
    consumer_gp_nm = "lfc_input_calc"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list)

    # Set booleam value used to determine if first loop of function
    is_first_loop = True

    log.info("Calculating LFC p_input when p_demand or p_correction changes....")

    # loop to calculate p_input (sum of p_demand and p_correction) and send to kafka, if p_demand and/or p_correction changed
    while True:
        # Save start time for loop
        time_loop_start = time()

        # check if topics which are both produced and consumed are empty, else init
        empty_consumed_and_produced_topics = kafka_obj.list_empty_consumed_and_produced_topics()
        for topic in empty_consumed_and_produced_topics:
            if topic == tp_nm.lfc_p_input_calc_state:
                log.info(f"Topic {topic} was empty. Initialised with default value.")
                kafka_obj.produce_message(topic_name=tp_nm.lfc_p_input_calc_state,
                                          msg_value={'Timestamp': str(datetime.now()),
                                                     msg_val_nm.last_lfc_p_dem: 0,
                                                     msg_val_nm.last_lfc_p_corr: 0,
                                                     msg_val_nm.lfc_p_input: 0})

        # check if consumed only data is availiable and wait if not, else do it
        empty_consumed_only_topics = kafka_obj.list_empty_consumed_only_topics()
        if empty_consumed_only_topics:
            log.warning(f"The consumed only topics: {empty_consumed_only_topics} are empty. Waiting for input data.")
            sleep(REFRESH_RATE_CALC_P_INPUT)
        else:
            # get latest messages from consumed topics
            msg_val_dict = kafka_obj.get_latest_topic_messages_to_dict_poll_based()
            # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

            current_lfc_p_corr = msg_val_dict[tp_nm.lfc_p_corr][msg_val_nm.lfc_p_corr]
            current_lfc_p_dem = msg_val_dict[tp_nm.lfc_p_dem][msg_val_nm.lfc_p_dem]

            last_lfc_p_corr = msg_val_dict[tp_nm.lfc_p_input_calc_state][msg_val_nm.last_lfc_p_corr]
            last_lfc_p_dem = msg_val_dict[tp_nm.lfc_p_input_calc_state][msg_val_nm.last_lfc_p_dem]

            # Calculate new LFC input and send to Kafka if:
            # LFC demand changed, LFC correction changed or it is first loop af function

            if last_lfc_p_dem != current_lfc_p_dem or last_lfc_p_corr != current_lfc_p_corr or is_first_loop:

                # Log if demand changed or remained the same
                if last_lfc_p_dem == current_lfc_p_dem:
                    log.debug(f"LFC p_demand is still: {current_lfc_p_dem}")
                else:
                    log.info(f"LFC p_demand changed to: {current_lfc_p_dem} from: {last_lfc_p_dem}")

                # Log if correction changed or remained the same
                if last_lfc_p_corr == current_lfc_p_corr:
                    log.debug(f"LFC p_correction is still: {current_lfc_p_corr}")
                else:
                    log.info(f"LFC p_correction changed to: {current_lfc_p_corr} from: {last_lfc_p_corr}")

                # Save cycle start time
                time_current_calc_start = datetime.now()

                # Calculate LFC p_input as a sum of p_demand and p_correction
                lfc_p_input = round(current_lfc_p_dem + current_lfc_p_corr, PRECISION_DECIMALS)

                log.info(f"LFC p_input is now: {lfc_p_input}")

                # Send LFC p_input to kafka
                kafka_obj.produce_message(topic_name=tp_nm.lfc_p_input,
                                          msg_value={msg_val_nm.lfc_p_input: lfc_p_input})

                # Send new state info for LFC p_input calculation to kafka
                kafka_obj.produce_message(topic_name=tp_nm.lfc_p_input_calc_state,
                                          msg_value={'Timestamp': str(time_current_calc_start),
                                                     msg_val_nm.last_lfc_p_dem: current_lfc_p_dem,
                                                     msg_val_nm.last_lfc_p_corr: current_lfc_p_corr,
                                                     msg_val_nm.lfc_p_input: lfc_p_input})

                log.debug(f"LFC p_input calculation done in: {round(time()-time_loop_start,3)} secounds.")

            is_first_loop = False

            # Sleep before next loop
            sleep(REFRESH_RATE_CALC_P_INPUT)
