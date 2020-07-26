package com.laozhang.redis.client;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

public class RedisClient {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.1.211",6379);

        System.out.println(jedis.ping());

        jedis.publish("xuanyutech","haha");
    }
}
