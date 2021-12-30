# Import dependencies
from functions_lfc import (add_to_log)


# bids
# TODO are bids split in up and down? (get list from someone?)
lmol_bids = [
    {
        "bid_mrid": "123",
        "bid_name": "DONG-W",
        "price_up": 50,
        "price_down": 40,
        "capacity_up": 50,
        "capacity_down": 50
    },
    {
        "bid_mrid": "456",
        "bid_name": "CENTRICA",
        "price_up": 40,
        "price_down": 50,
        "capacity_up": 50,
        "capacity_down": 50
    }
]

# class blueprint
class BidServiceProvider:
    # init/constructor
    def __init__(self, bid_mrid, bsp_name, price_up, price_down, capacity_up, capacity_down):
        # attributes
        # todo more bids can be availiable per BSP
        self.bid_mrid = bid_mrid
        self.bsp_name = bsp_name
        self.price_up = price_up
        self.price_down = price_down
        self.capacity_up = capacity_up
        self.capacity_down = capacity_down
        self.setpoint = 0
        self.activated = False
        self.availiable = True
        add_to_log(f"BSP: {self.bsp_name} ready.")
    
    # methods
    # - activate bid
    def activate_bid(self):
        self.activated = True    
        
    # - deactivate bid
    def deactivate_bid(self):
        self.activated = False

""" 
TODO:
Now:
- Load bids from list of dicts
- make dict with object for each brp/bid

Later:
- Load bid list from file to Kafka (MOL)
- Load bid list from Kafka
"""

#lmol_bids_sorted_up = sorted(lmol_bids, key=lambda k: k['price_up']) 
#lmol_bids_sorted_down = sorted(lmol_bids, key=lambda k: k['price_down']) 





#dong = BidServiceProvider("123","DONG-W", "40", "40", "100", "100")
#add_to_log(f"{dong.bsp_name} has price down: {dong.price_down}")

# print(dong.__dict__)

deadband = 1

if __name__ == "__main__":
    
    # make list of bsp based on lmol
    bid_list = []
    for bid in lmol_bids:
        bid_list.append(BidServiceProvider(bid["bid_mrid"],bid["bid_name"],bid["price_up"],bid["price_down"],bid["capacity_up"],bid["capacity_down"]))
    
    p_target = -51
    
    # sort bids by either up or down prices dependent on lfc need
    reg_up = False
    reg_down = False
    if p_target > deadband:
        # sort list by cheapest price for up regulation
        reg_up = True
        add_to_log(f"Up regulation needed.")
        bid_list.sort(key=lambda x: x.price_up, reverse=False)
    elif p_target < -deadband:
        # sort list by cheapest price for down regulation
        reg_down = True
        add_to_log(f"Down regulation needed.")
        bid_list.sort(key=lambda x: x.price_down, reverse=False)
        
    # mark necessary bids as activated
    if reg_up or reg_down:
        availiable_regulation = 0
        remaning_regulation = abs(p_target)
        for bsp in bid_list:
            add_to_log(f"BSP bid from {bsp.bsp_name} neeeded.")
            bsp.activate_bid()
            if reg_up:
                availiable_regulation += bsp.capacity_up
                setpoint = (min(remaning_regulation,bsp.capacity_up))
                remaning_regulation -= setpoint
            elif reg_down:
                availiable_regulation += bsp.capacity_down
                setpoint = (min(remaning_regulation,bsp.capacity_down))
                remaning_regulation -= setpoint
            
            bsp.setpoint = setpoint

            
            #
            if availiable_regulation >= abs(p_target):
                break
    

    # TODO deactivate bids which are no longer used (do when distributing target or handle elsewhere?)
    # distribute target on activated bids
    # TODO how to handle ramping down?
    for bsp in bid_list:
        if bsp.activated:
            add_to_log(f"{bsp.bsp_name} activated {bsp.setpoint}")