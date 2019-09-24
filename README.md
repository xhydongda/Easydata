# Easydata
A tailored version of influxdb engine with data structure fixed as {sid, timestamp, value, qualify} commonly used in IOT, based on .net core3.0.

You call run DataSim to test the engine.

## Features to be done:
1. Cluster version based on MS Orleans or...
2. Lossy compression before write.
3. Multi intervals and calculators, for examples, (3600,average), (86400,maximum)...
4. Cache reading with interval >= 1 hour.
5. DataX to provide commonly used industrial protocols such as Modbus, OPC...

## License
MIT License

Copyright (c) 2019 Yideyun
