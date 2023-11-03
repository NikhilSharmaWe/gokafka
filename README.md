# gokafka
If a order processor is down, the orders can stack up in the kafka and resume when the processor is back up.
If we want to process same data multiple times for different purposes we can replay the whole thing again.