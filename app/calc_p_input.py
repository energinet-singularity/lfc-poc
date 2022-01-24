# Import dependencies
from time import sleep, time
from datetime import datetime

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

if __name__ == "__main__":

    add_to_log("Info: LFC p_input calculation initializing..")

    # Create lists of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_input_calc_state, tp_nm.lfc_p_input]
    topics_consumed_list = [tp_nm.lfc_p_corr, tp_nm.lfc_p_dem, tp_nm.lfc_p_input_calc_state]

    # init kafka
    consumer_gp_nm = "lfc_input_calc"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    # Set booleam value used to determine if first loop of function
    is_first_loop = True

    add_to_log("Info: Calculating LFC p_input when p_demand or p_correction changes....")

    # loop to calculate p_input (sum of p_demand and p_correction) and send to kafka, if p_demand and/or p_correction changed
    while True:
        # Save start time for loop
        time_loop_start = time()

        # get latest messages from consumed topics
        msg_val_dict = kafka_obj.get_latest_msg_from_consumed_topics_to_dict()
        # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # TODO check possibility/need for default value and rounding
        current_lfc_p_corr = msg_val_dict[tp_nm.lfc_p_corr][msg_val_nm.lfc_p_corr]
        current_lfc_p_dem = msg_val_dict[tp_nm.lfc_p_dem][msg_val_nm.lfc_p_dem]
        last_lfc_p_corr = msg_val_dict[tp_nm.lfc_p_input_calc_state][msg_val_nm.last_lfc_p_corr]
        last_lfc_p_dem = msg_val_dict[tp_nm.lfc_p_input_calc_state][msg_val_nm.last_lfc_p_dem]

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

            # Send LFC p_input to kafka
            kafka_obj.produce_message(topic_name=tp_nm.lfc_p_input,
                                      msg_value={msg_val_nm.lfc_p_input: lfc_p_input})

            # Send new state info for LFC p_input calculation to kafka
            kafka_obj.produce_message(topic_name=tp_nm.lfc_p_input_calc_state,
                                      msg_value={'Timestamp': str(time_current_calc_start),
                                                 msg_val_nm.last_lfc_p_dem: current_lfc_p_dem,
                                                 msg_val_nm.last_lfc_p_corr: current_lfc_p_corr,
                                                 msg_val_nm.lfc_p_input: lfc_p_input})

            add_to_log(f"Debug: LFC p_input calculation done in: {round(time()-time_loop_start,3)} secounds.")

        is_first_loop = False

        # Sleep before next loop
        sleep(PARM.REFRESH_RATE_MS_LFC_INPUT/1000)
