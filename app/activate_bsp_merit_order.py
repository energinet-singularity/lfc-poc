# Import dependencies
from time import sleep, time
from json import loads

# Import functions
from functions_kafka_class import KafkaHelper
from functions_lfc import add_to_log

# Import parameters, Kafka topic names and message value names
import parm_kafka_topic_nm as tp_nm
import parm_kafka_msg_val_nm as msg_val_nm
import parm_general as PARM

"""
TODO:
- kombiner table for bsp og bid act
- Split main i moduler
- Run via docker compose (alle pånær lmol parser, da det kræver fil mount)
- Dokumenter classes ordentligt
- PBR simulering laves så der er en for hver BSP
- Lav kafka topics som env vars

"""

# BSP class blueprint
class BSP:
    # init/constructor
    def __init__(self, mrid):
        # attributes
        self.mrid = mrid
        self.setpoint = 0

    # methods
    # - set setpint
    def set_setpoint(self, setpoint):
        self.setpoint = round(setpoint, PARM.PRECISION_DECIMALS)

    # - set setpint
    def add_to_setpoint(self, setpoint):
        self.setpoint += round(setpoint, PARM.PRECISION_DECIMALS)


# Bid class blueprint
class Bid:
    # init/constructor
    def __init__(self, mrid_bsp, mrid_bid, name, direction, price, capacity, availiable):
        # attributes
        self.mrid_bsp = mrid_bsp
        self.mrid_bid = mrid_bid
        self.name = name
        self.direction = direction
        self.price = float(price)
        self.capacity = float(capacity)
        self.availiable = bool(availiable)
        self.activated = False

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
    topics_consumed_list = [tp_nm.lfc_p_target, tp_nm.lfc_bsp_lmol]

    # init kafka
    consumer_gp_nm = "lfc_bsp_activation"
    kafka_obj = KafkaHelper(group_id=consumer_gp_nm,
                            auto_offset_reset="earliest",
                            enable_auto_commit=False,
                            topics_consumed_list=topics_consumed_list,
                            topics_produced_list=topics_produced_list,
                            poll_timeout_ms=PARM.TIMEOUT_MS_POLL)

    add_to_log("Activating BSP in merit order baes on LMOL and P_target..")
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
        # TODO skal ikke poll hver gang med må gerne trigge poll? evt. kun på topic som skal have data frem for at polle alle?
        # TODO return via method i stedet for attribute på class?
        kafka_obj.get_msg_val_from_dict(tp_nm=tp_nm.lfc_p_target,
                                        msg_val_nm=msg_val_nm.lfc_p_target,
                                        default_val=0)
        p_target = kafka_obj.message_value

        # get lmol
        kafka_obj.get_msg_val_from_dict(tp_nm=tp_nm.lfc_bsp_lmol,
                                        msg_val_nm=msg_val_nm.lfc_bsp_lmol)

        lmol = loads(kafka_obj.message_value)
        lmol.sort(key=lambda x: (x['direction'], x['price']), reverse=False)

        # calculate BSP activation of bids if p_target has changed
        if last_p_target != p_target:

            last_p_target = p_target

            # make list of bids based on lmol (local merit order list) and make list of BSP's
            # TODO make function
            bid_list = []
            bsp_list = []
            
            # make list of objects
            for bid in lmol:
                bid_list.append(Bid(mrid_bsp=bid["mrid_bsp"],
                                    mrid_bid=bid["mrid_bid"],
                                    name=bid["name"],
                                    direction=bid["direction"],
                                    price=bid["price"],
                                    capacity=bid["capacity"],
                                    availiable=bid["availiable"]))

                # add bsp to bsp list
                if not any(x.mrid == bid["mrid_bsp"] for x in bsp_list):
                    bsp_list.append(BSP(mrid=bid["mrid_bsp"]))

            # sort bids by either up or down prices dependent on lfc regulation need defined by target
            # TODO make as function
            reg_up = False
            reg_down = False
            bid_list_down = []
            bid_list_up = []
            if p_target > 0:
                # extract up regualtion bids and sort list by cheapest price
                reg_up = True
                bid_list_up = [i for i in bid_list if (i.direction == 'UP')]
                bid_list_up.sort(key=lambda x: x.price, reverse=False)
                add_to_log(f"Info: {abs(p_target)} MW up regulation needed.")
            elif p_target < 0:
                # extract up regualtion bids and sort list by cheapest price
                reg_down = True
                bid_list_down = [i for i in bid_list if (i.direction == 'DOWN')]
                bid_list_down.sort(key=lambda x: x.price, reverse=False)
                add_to_log(f"Info: {abs(p_target)} MW down regulation needed.")
            else:
                add_to_log(f"Info: No regulation needed. Target is {p_target}.")

            # mark necessary bids as activated
            # TODO make as function
            if reg_up or reg_down:
                availiable_regulation = 0
                if reg_up:
                    availiable_bids = bid_list_up
                elif reg_down:
                    availiable_bids = bid_list_down
                
                remaning_regulation = round(abs(p_target), PARM.PRECISION_DECIMALS)
                for bid in availiable_bids:

                    add_to_log(f"Info: BSP bid '{bid.mrid_bid}' from {bid.name} neeeded.")
                    bid.activate_bid()

                    if reg_up:
                        availiable_regulation += bid.capacity
                        setpoint = round((min(remaning_regulation, bid.capacity)), PARM.PRECISION_DECIMALS)
                        remaning_regulation -= setpoint
                        # add setpoint to bsp_mrid
                        # TODO make smarter
                        for bsp in bsp_list:
                            if bsp.mrid == bid.mrid_bsp:
                                bsp.add_to_setpoint(setpoint)
                    elif reg_down:
                        availiable_regulation += bid.capacity
                        setpoint = round((min(remaning_regulation, bid.capacity)), PARM.PRECISION_DECIMALS)
                        remaning_regulation -= setpoint
                        # add setpoint to bsp_mrid
                        # TODO make smarter
                        for bsp in bsp_list:
                            if bsp.mrid == bid.mrid_bsp:
                                bsp.add_to_setpoint(-setpoint)

                    # if availiable regulation from bids is over targer, then break loop
                    if availiable_regulation >= abs(p_target):
                        break

            for bsp in bsp_list:
                add_to_log(f"Info: Setpoint set to: {bsp.setpoint} for '{bsp.mrid}'")

            kafka_obj.produce_message(topic_name=tp_nm.lfc_bsp_activated,
                                      msg_value={msg_val_nm.lfc_bsp_activated: [bsp.__dict__ for bsp in bsp_list]})

            add_to_log(f"Debug: BSP activation done in: {round(time()-time_loop_start,3)} secounds.")
