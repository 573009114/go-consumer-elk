# go-consumer-elk
## 功能
日志消费工具

## 流程
日志收集工具收集->输出到kafka-> 工具消费kafka数据->写入es

## 日志收集工具 logstash 配置参考
```
input {
    file {
        path => "/var/test/log/smart/*.log"
	    type => "biz-test-log1"
	    codec => json {}
    	}
}

filter {
    if [type] == "biz-test-log1" {
        json {
            source => "message"
        }
    }
}
output {

    kafka {
         bootstrap_servers => "10.10.3.12:9092"
         topic_id  => ["biz-test-log1"]
         codec => json {}
    }
}

```