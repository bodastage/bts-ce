#rabbitmqctl add_user btsuser Password@7
#rabbitmqctl set_user_tags btsuser administrator
#rabbitmqctl add_vhost /bs
#rabbitmqctl set_permissions -p /bs btsuser ".*" ".*" ".*"
#rabbitmqctl delete_user guest

# Enable stomp
rabbitmq-plugins enable rabbitmq_web_stomp


# Re-start service
rabbitmq-server stop
rabbitmq-server start
