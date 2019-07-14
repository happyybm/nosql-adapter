# sql-nosql

# Version 1.0

# Description
>sql的结构化查询方式
>model层的封装CRUD方法

# 示例
<pre><code>
$select = new DynamoDbSelect();
$select->from("tableName")->from("index_name")->where("filed1=:f1:",array("f1"=>"value1"))->where("filed2=:f2:",array("f2"=>"value2"));
$options=[];//options
$db = new DynamoDb($options);
$rs = $db->fetchOne($select);
</code>
</pre>
