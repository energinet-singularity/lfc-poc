# Import depedencies
from prettytable import PrettyTable
from time import sleep, time
from json import loads

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log, print_bid_logo_doh

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

bid_table = PrettyTable()
bid_table.field_names = ["mrid_bsp", "mrid_bid", "name", "direction", "price", "capacity", "availiable"]

if __name__ == "__main__":

    # Create lists of topic names produced to and consumed from
    topics_consumed_list = [tp_nm.lfc_bsp_lmol]

    # init kafka
    kafka_obj = KafkaHelper(group_id="None",
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    while True:

        bid_table.clear_rows()

        # time of loop start
        time_loop_start = time()

        # Report warnings on empty topics ?
        kafka_obj.list_empty_topics()

        # Get latest value for each comnsumed topic
        kafka_obj.get_latest_topic_messages_to_dict_poll_based()

        # get  value
        kafka_obj.get_msg_val_from_dict(tp_nm=tp_nm.lfc_bsp_lmol,
                                        msg_val_nm=msg_val_nm.lfc_bsp_lmol)

        # TODO sort by price up og down dependent on p_targer
        data = loads(kafka_obj.message_value)
        data.sort(key=lambda x: (x['direction'], x['price']), reverse=False)

        # Fill table
        for bid in data:
            #print(type([bid["mrid_bsp"], bid["mrid_bid"], bid["name"], bid["direction"], bid["price"], bid["availiable"]]))
            # TODO loop list og lav table der ud fra
            bid_table.add_row([bid["mrid_bsp"], bid["mrid_bid"], bid["name"], bid["direction"], bid["price"], bid["capacity"], bid["availiable"]])
            # bid_table.add_row([bid["mrid_bsp"], bid["mrid_bid"], bid["name"], bid["direction"], bid["price"], bid["availiable"]])

        # display table
        print_bid_logo_doh()
        add_to_log("")
        bid_table.align = "l"
        bid_table.junction_char
        print(bid_table)
        print("\n")

        sleep(1)
