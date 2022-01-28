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
## 优势
传统的logstash 处理消费数据，对cpu和内存的消耗巨大。并且logstash做为消费工具，偏重。
用这个工具可以提高效率
## 后续计划
增加报警功能
