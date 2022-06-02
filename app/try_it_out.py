import singukafka
import logging
from time import time, sleep

# Initialize log
log = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)

log.info("starting")
time_start = time()

consumer_gp_nm = "singutesterlige12"
kafka_obj = singukafka.KafkaHelper(group_id=consumer_gp_nm,
                                   auto_offset_reset="earliest",
                                   enable_auto_commit=False,
                                   topics_consumed_list=['lfc-mw.diff.value', 'lfc-bsp.activated.value'],
                                   topics_produced_list=[],
                                   poll_timeout_ms=0,
                                   fetch_max_wait_ms=0)

for i in range(2):

    time_start = time()
    # dataframe = kafka_obj.get_kafka_messages_to_pandas_dataframe(msg_key_filter="lfc_mw_diff", get_latest_msg_by_key=True, get_latest_produced_msg_only=False)
    dataframe = kafka_obj.get_kafka_messages_to_pandas_dataframe(msg_key_filter="lfc_mw_diff",get_latest_msg_by_key=True, get_latest_produced_msg_only=True)
    print(f"Data extract nr. {i+1} took: {round(time()-time_start,3)} secounds.")
    print("Data is:")
    print(dataframe)
