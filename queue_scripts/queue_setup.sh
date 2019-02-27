#!/bin/bash

# Enable stomp
rabbitmq-plugins enable rabbitmq_web_stomp

# Re-start service
rabbitmq-server stop
rabbitmq-server start

