# Import dependencies
from time import sleep, time

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

""" 
TODO:
This module:
- Run via docker compose
- Load bid list from Kafka (when lmol parser done)

Module "lmol_parser"
- Load bid list from file to Kafka (MOL)

Module ui for bud visning (se papir)

PBR simulering laves så der er en for hver BSP

"""

# bids
# TODO are bids split in up and down? (get list from someone?)
# TODO bids from kafka instead
lmol_bids = [
    {
        "bsp_mrid": "DONG-W_MRID",
        "bsp_name": "DONG-W",
        "bid_mrid": "123",
        "price_up": 50,
        "price_down": 40,
        "capacity_up": 50,
        "capacity_down": 50
    },
    {
        "bsp_mrid": "DONG-W_MRID",
        "bsp_name": "DONG-W",
        "bid_mrid": "321",
        "price_up": 10,
        "price_down": 10,
        "capacity_up": 10,
        "capacity_down": 10
    },
    {
        "bsp_mrid": "CENTRICA_MRID",
        "bsp_name": "CENTRICA",
        "bid_mrid": "456",
        "price_up": 40,
        "price_down": 30,
        "capacity_up": 30,
        "capacity_down": 30
    }
]

# BSP class blueprint
class BSP:
    # init/constructor
    def __init__(self, bsp_mrid):
        # attributes
        self.bsp_mrid = bsp_mrid
        self.setpoint = 0
    
    # methods
    # - set setpint
    def set_setpoint(self, setpoint):
        self.setpoint = round(setpoint,PARM.PRECISION_DECIMALS)
    # - set setpint
    def add_to_setpoint(self, setpoint):
        self.setpoint += round(setpoint,PARM.PRECISION_DECIMALS)

# Bid class blueprint
class Bid:
    # init/constructor
    def __init__(self, bsp_mrid, bsp_name, bid_mrid, price_up, price_down, capacity_up, capacity_down):
        # attributes
        self.bsp_mrid = bsp_mrid
        self.bsp_name = bsp_name
        self.bid_mrid = bid_mrid
        self.price_up = price_up
        self.price_down = price_down
        self.capacity_up = capacity_up
        self.capacity_down = capacity_down
        self.activated = False
        self.availiable = True
    
    # methods
    # - activate bid
    def activate_bid(self):
        self.activated = True    
        
    # - deactivate bid
    def deactivate_bid(self):
        self.activated = False


last_p_target = None

if __name__ == "__main__": 

    # Create lists of topic names produced to and consumed from
    topics_produced_list = [tp_nm.lfc_bsp_activated]
    topics_consumed_list = [tp_nm.lfc_p_target]  # , tp_nm.lfc_bsp_lmol]

    # init kafka
    consumer_gp_nm = "lfc_bsp_activation"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    add_to_log("Activating BSP in merit order baes on MOL and P_target..")
    add_to_log("|-----------------------------------------|")

    while True:
        # TODO is sleep needed?
        sleep(0.1)

        # time of loop start
        time_loop_start = time()

        # Report warnings on empty topics ?
        kafka_obj.list_empty_topics()

        # Get latest value for each comnsumed topic
        kafka_obj.get_latest_topic_messages_to_dict_poll_based()
        # add_to_log(f"Debug: Getting messages took: {round(time()-time_loop_start,3)} secounds.")

        # get p_target value
        # TODO (byd denne ind i class, men hvordan smartest? skal ikke poll hver gang med må gerne trigge poll? evt. kun på topic som skal have data frem for at polle alle consumed?)
        # TODO return via method i stedet for attribute på class?
        kafka_obj.get_msg_val_from_dict(tp_nm=tp_nm.lfc_p_target,
                                        msg_val_nm=msg_val_nm.lfc_p_target,
                                        default_val=0)
        p_target = kafka_obj.message_value

        # calculate BSP activation of bids if p_target has changed
        if last_p_target != p_target:

            last_p_target = p_target

            # make list of bids based on lmol (local merit order list) and make list of BSP's
            bid_list = []
            bsp_list = []
            for bid in lmol_bids:
                bid_list.append(Bid(bsp_mrid=bid["bsp_mrid"],
                                    bsp_name=bid["bsp_name"],
                                    bid_mrid=bid["bid_mrid"],
                                    price_up=bid["price_up"],
                                    price_down=bid["price_down"],
                                    capacity_up=bid["capacity_up"],
                                    capacity_down=bid["capacity_down"]))

                # add bsp to bsp list
                if not any(x.bsp_mrid == bid["bsp_mrid"] for x in bsp_list):
                    bsp_list.append(BSP(bsp_mrid=bid["bsp_mrid"]))

            # sort bids by either up or down prices dependent on lfc regulation need defined by target
            reg_up = False
            reg_down = False
            if p_target > 0:
                # sort list by cheapest price for up regulation
                reg_up = True
                add_to_log(f"Info: {abs(p_target)} MW up regulation needed.")
                bid_list.sort(key=lambda x: x.price_up, reverse=False)
            elif p_target < 0:
                # sort list by cheapest price for down regulation
                reg_down = True
                add_to_log(f"Info: {abs(p_target)} MW down regulation needed.")
                bid_list.sort(key=lambda x: x.price_down, reverse=False)
            else:
                add_to_log(f"Info: No regulation needed. Target is {p_target}.")

            # mark necessary bids as activated
            if reg_up or reg_down:
                availiable_regulation = 0
                remaning_regulation = round(abs(p_target), PARM.PRECISION_DECIMALS)
                for bid in bid_list:
                    add_to_log(f"Info: BSP bid '{bid.bid_mrid}' from {bid.bsp_name} neeeded.")
                    bid.activate_bid()
                    if reg_up:
                        availiable_regulation += bid.capacity_up
                        setpoint = round((min(remaning_regulation, bid.capacity_up)), PARM.PRECISION_DECIMALS)
                        remaning_regulation -= setpoint
                        # add setpoint to bsp_mrid
                        # TODO make smarter
                        for bsp in bsp_list:
                            if bsp.bsp_mrid == bid.bsp_mrid:
                                bsp.add_to_setpoint(setpoint)
                    elif reg_down:
                        availiable_regulation += bid.capacity_down
                        setpoint = round((min(remaning_regulation, bid.capacity_down)), PARM.PRECISION_DECIMALS)
                        remaning_regulation -= setpoint
                        # add setpoint to bsp_mrid
                        # TODO make smarter
                        for bsp in bsp_list:
                            if bsp.bsp_mrid == bid.bsp_mrid:
                                bsp.add_to_setpoint(-setpoint)

                    # if availiable regulation from bids is over targer, then break loop
                    if availiable_regulation >= abs(p_target):
                        break

            for bsp in bsp_list:
                add_to_log(f"Info: Setpoint set to: {bsp.setpoint} for '{bsp.bsp_mrid}'")

            kafka_obj.produce_message(topic_name=tp_nm.lfc_bsp_activated, msg_value={msg_val_nm.lfc_bsp_activated: [bsp.__dict__ for bsp in bsp_list]})

            add_to_log(f"Debug: BSP activation done in: {round(time()-time_loop_start,3)} secounds.")
