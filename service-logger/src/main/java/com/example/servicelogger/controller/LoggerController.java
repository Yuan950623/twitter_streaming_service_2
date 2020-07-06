package com.example.servicelogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "/log", method = RequestMethod.POST)
    public String dolog(@RequestParam("log") String currLog) {
        JSONObject jsonObject = JSON.parseObject(currLog);
        jsonObject.put("ts", System.currentTimeMillis());
        logger.info(jsonObject.toJSONString());
        System.out.println(jsonObject.toString());

        kafkaTemplate.send("twitter", jsonObject.toJSONString());

        return "success!";
    }
}
