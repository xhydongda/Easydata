# Easydata
A tailored version of [influxdb](https://github.com/influxdata/influxdb) engine with data structure fixed as {sid, timestamp, value, qualify} commonly used in IOT, based on .net core3.0. 
## Publications
[基于influxDB的工业时序数据库引擎设计](https://github.com/xhydongda/Easydata/blob/master/doc/%E5%9F%BA%E4%BA%8EinfluxDB%E7%9A%84%E5%B7%A5%E4%B8%9A%E6%97%B6%E5%BA%8F%E6%95%B0%E6%8D%AE%E5%BA%93%E5%BC%95%E6%93%8E%E8%AE%BE%E8%AE%A1.pdf).

## How to use
Using vs2019 v16.3 to open the .sln file, debug/release DataSim, manually copy SnappyDL.x64.dll to output directory, then run DataSim.exe.

First input number of points per batch and Enter, 50,000 is default;
Then input the interval to generate random data an write, and Enter, 1,000 ms is default;
Enter.

## Performance
Span<T>, ArrayPool, unsafe are used.
With 50,000 points of double/int64/bool writing, the engine uses less then 100ms.
With one point one hour reading, the engine uses about 1ms.

## Easydata vs influxdb
1. c# vs go;
2. {ulong sid, timestamp, value, quality} vs {string key, timestamp, value}
3. different(straightforward) reading implemetation:
   each shard covers same interval(1 hour, 1 day, 1 month);
   find shards overlap the reading range, and call shard.Read.

## Compression rate
With random data, we got:
double  68.74%
int64   69.67%
bool    92.53%。

With 5000 points real industy data, we got 89.1%.


## Features to be done:
1. Cluster version based on MS Orleans or...
2. Lossy compression before write.
3. Multi intervals and calculators, for examples, (3600,average), (86400,maximum)...
4. Cache reading with interval >= 1 hour.
5. DataX to provide commonly used industrial protocols such as Modbus, OPC...

## License
MIT License

Copyright (c) 2019 Yideyun
