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
# TODO include as default setting instead and make possible to change
PRECISION_DECIMALS = 2
CYCLETIME_S_LFC = 4
REFRESH_RATE_WAIT_FOR_KAFKA_DATA = 1
SETPOINT_LFC_P_INPUT = 0
KP = 0.01
KI = 0.02
KD = 0
DEADBAND_LFC_ERROR = 0.5

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S.%03d')
logging.getLogger().setLevel(logging.WARNING)


def lfc_pid_controller(meas: float, last_error: float, last_error_sum: float, cycletime_s: float):
    """
    PID controller calculating target based on PID-settings, measrurement and setpoint.
    """
    error = SETPOINT_LFC_P_INPUT - meas

    if abs(error) < DEADBAND_LFC_ERROR:
        error = 0
        log.info(f"LFC error is respecting deadband: {DEADBAND_LFC_ERROR}")

    error_sum = last_error_sum + (error * cycletime_s)
    error_diff = (error - last_error) / cycletime_s

    target = round(KP * error + KI * error_sum + KD * error_diff, PRECISION_DECIMALS)

    return target, error, error_sum, error_diff


if __name__ == "__main__":

    log.info(f"LFC p_target calculation initializing..")

    # Create lists of topic names (produced to, consumed from and combined list)
    topics_produced_list = [tp_nm.lfc_p_target, tp_nm.lfc_p_target_state]
    topics_consumed_list = [tp_nm.lfc_p_input, tp_nm.lfc_p_target_state]

    # init kafka
    consumer_gp_nm = "lfc_target_calc"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list)

    log.info("Calculating LFC p_target by use of PID-controller..")

    """
    TODO describe
    """
    while True:
        # Save start time for loop
        time_loop_start = time()

        # check if topics which are both produced and consumed are empty, else init)
        empty_consumed_and_produced_topics = kafka_obj.list_empty_consumed_and_produced_topics()
        for topic in empty_consumed_and_produced_topics:
            if topic == tp_nm.lfc_p_target_state:
                log.info(f"Topic {topic} was empty. Initialised with default value.")
                kafka_obj.produce_message(topic_name=tp_nm.lfc_p_target_state,
                                          msg_value={'Timestamp': str(datetime.now()),
                                                     msg_val_nm.lfc_p_target: 0,
                                                     msg_val_nm.lfc_p_target_error: 0,
                                                     msg_val_nm.lfc_p_target_error_sum: 0,
                                                     msg_val_nm.lfc_p_target_error_diff: 0})

        # check if consumed only data is availiable and wait if not, else do it
        empty_consumed_only_topics = kafka_obj.list_empty_consumed_only_topics()
        if empty_consumed_only_topics:
            log.warning(f"The consumed only topics: {empty_consumed_only_topics} are empty. Waiting for input data.")
            sleep(REFRESH_RATE_WAIT_FOR_KAFKA_DATA)
        else:
            # get latest messages from consumed topics
            msg_val_dict = kafka_obj.get_latest_topic_messages_to_dict_poll_based()

            current_lfc_p_input = msg_val_dict[tp_nm.lfc_p_input][msg_val_nm.lfc_p_input]
            last_error = msg_val_dict[tp_nm.lfc_p_target_state][msg_val_nm.lfc_p_target_error]
            last_error_sum = msg_val_dict[tp_nm.lfc_p_target_state][msg_val_nm.lfc_p_target_error_sum]

            log.info(f"LFC p_input is: {current_lfc_p_input}. Calculating p_target..")

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
                                                                            cycletime_s=CYCLETIME_S_LFC)
            log.info(f"LFC p_target is: {lfc_p_target}")

            # Send state of calculation to Kakfa foir usage in enxt cycle
            kafka_obj.produce_message(topic_name=tp_nm.lfc_p_target_state,
                                      msg_value={'Timestamp': str(time_current_cycle_start),
                                                 msg_val_nm.lfc_p_target: lfc_p_target,
                                                 msg_val_nm.lfc_p_target_error: error,
                                                 msg_val_nm.lfc_p_target_error_sum: error_sum,
                                                 msg_val_nm.lfc_p_target_error_diff: error_diff})

            # Send LFC p_target value to topic
            kafka_obj.produce_message(topic_name=tp_nm.lfc_p_target,
                                      msg_value={msg_val_nm.lfc_p_target: lfc_p_target})

            sleep(CYCLETIME_S_LFC)
