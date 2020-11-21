Create the IP aliases first

```bash
sudo ifconfig lo0 alias 127.0.0.2
sudo ifconfig lo0 alias 127.0.0.3
```

Then add those entries to our /etc/hosts

```bash
127.0.0.1	kafka-1
127.0.0.2	kafka-2
127.0.0.3	kafka-3
```