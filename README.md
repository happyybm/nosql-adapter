# sql-nosql
>非关系型数据库结构化查询方式

## 安装
* 通过composer
```bash
$ composer require nosql/adapter
```
## 目前支持的查询操作
```php
=,!=,>,<,>=,<=,IS NOT NULL,IS NULL,IN,NOT IN,LIKE 
```
## 目前支持的适配器
ElasticSearch, DynamoDB

# 示例
```php
//配置信息，不同适配器配置内容不同
// DynamoDB配置：
// $options=[
//     "version" => "latest",
//      "keyid" => "keyid",
//      "access_key" => "key",
//      "region" => "region"
// ];

//ES配置
$options=[
    "host"=>"hostname",
    "port"=>"9200"
];
$page = 0;
$pageSize = 100;
$skip = $page*$pageSize;
$db = Db::factory("ElasticSearch",$options);
$select = $db->select();
$select->from("tableName")->from("index_name")->where("filed1=:f1:",array("f1"=>"value1"));
$select->where("filed2=?","value2")->where("field is not null");
$select->orWhere("filed2=:v2:",["v2"=>"value2"]);
//三个以上where时，orWhere会有优先级问题，可以使用()，例：
$select->where("field=? and (field=? or field=?) ",array("value1","value2","value3"));
$select->inWhere("field",$values)->orderBy(["filed"=>"desc","field2"=>"asc"]])->limit($page,$skip);
$rs = $db->fetchOne($select);
```
