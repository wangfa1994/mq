package com.wf.a03.a03Avro;

public class AvroSerializerMain {
    /*  avro最初的模式：
    {"namespace":"customerManagement.avro",
    "type":"record",
    "name":"Customer",
    "fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"},
        {"name":"faxNumber","type":["null","string"],"default":"null"}
        ]
    }
    id name 必须，faxNumber可选，默认null
    当我们不再需要faxNumber字段的时候，而添加email字段的时候，需要进行修改模式
    {"namespace":"customerManagement.avro",
    "type":"record",
    "name":"Customer",
    "fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"},
        {"name":"email","type":["null","string"],"default":"null"}
        ]
    }


    更新到新模式后，旧记录仍然包含 faxNumber 字段，而新记录则包含 email 字段，
    需要考虑如何让还在使用 faxNumber 字段的旧应用程序和已经使用 email 字段的新应用程序正常处理 Kafka 中的所有消息。 // 类似dubbo的接口定义？

    读取消息的应用程序会调用类似 getName()、getId() 和 getFaxNumber() 这样的方法。如果读取到使用新模式构建的消息，
    那么 getName() 方法和 getId() 方法仍然能够正常返回，但getFaxNumber() 方法将返回 null，因为消息里不包含传真号码

    现在，假设升级了应用程序，用 getEmail() 方法替代了 getFaxNumber() 方法。如果读取到一个使
    用旧模式构建的消息，那么 getEmail() 方法将返回 null，因为旧消息不包含邮件地址.


    Avro 的好处了：即使在未更新所有读取消息的应用程序的情况下修改了模式，也不会遇到异常或阻断性错误，也不需要修改已有的数据

    用于写入数据和读取数据的模式必须是相互兼容的。Avro 的文档提到了一些兼容性原则
    反序列化器需要获取写入数据时使用的模式，即使它可能与读取数据时使用的模式不一样
    * */
}
