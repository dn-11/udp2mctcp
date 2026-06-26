# udp2mctcp

transports UDP packets in multi-connection TCP.

Designed for tunnel ip over tcp.

## Usage

```bash
./mctcp -l udp://listen_udp_ip:port mctcp://mctcp_host:mctcp_port
./mctcp2udp -l mctcp://mctcp_host:mctcp_port -f udp://dial_udp_ip:port
```

## Example

如果中间需要代理，可以搭配 gost 使用

```bash
./mctcp -l udp://127.0.0.1:12345 -f mctcp://127.0.0.1:12346
./gost -L=tcp://127.0.0.1:12346/<remote_host>:<remote_port> -F=socks5://<socks5_host>:<socks5_port>
./mctcp -l mctcp://0.0.0.0:listen_port -f udp://<udp_host>:<udp_port>
```

## SpeedTest

经过测试，原速率为 2-4Mbps 的链路使用 32 个 TCP 连接的 mctcp 替换单线 tcp 测得到 iperf3 60-100 Mbps

iperf(tcp) over wireguard over mctcp over sock5

> iperf -> wireguard -> udp2mctcp -> gost -> socke5 server -> mctcp2udp -> wireguard -> iperf
> 
> ![img.png](img.png)