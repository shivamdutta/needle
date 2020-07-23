# Needle

Module to execute trades using Oathkeeper's strategy built on top on Kite package by Zerodha.

## Oathkeeper's strategy

According to this strategy, trades are executed for instruments whose current open price is higher than the previous day's highest price or the current open price is lower than the previous day's lowest price.
Also, a reinforcement mechanism is implemented to counter the losses that might be incurred.

## Cronjobs

```
*/15 4-8 * * 1-5 cd /home/ec2-user/needle && python3 place_child_order.py
0 9 * * 1-5 cd /home/ec2-user/needle && python3 place_child_order.py
15 9 * * 1-5 cd /home/ec2-user/needle && python3 place_child_order.py
30 9 * * 1-5 cd /home/ec2-user/needle && python3 place_child_order.py
30 4-9 * * 1-5 cd /home/ec2-user/needle && python3 positions_updater.py
30 13 * * * cd /home/ec2-user/needle && python3 automate.py
30 16 * * * cd /home/ec2-user/needle && python3 automate.py
30 18 * * * cd /home/ec2-user/needle && python3 automate.py
30 1 * * * cd /home/ec2-user/needle && python3 automate.py
30 2 * * * cd /home/ec2-user/needle && python3 automate.py
30 3 * * * cd /home/ec2-user/needle && python3 automate.py
39 3 * * * cd /home/ec2-user/needle && python3 data_today_09_09.py && python3 quantity_09_09.py
44 3 * * * cd /home/ec2-user/needle && python3 place_order_09_15.py
40 9 * * * cd /home/ec2-user/needle && python3 exit_order_15_10.py
10 10 * * * cd /home/ec2-user/needle && python3 fetch_order_15_40.py && python3 update_trades_15_40.py
30 12 * * * cd /home/ec2-user/needle && python3 data_prev_18_00.py
```