<?php
namespace Nosql\Adapter;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Credentials\Credentials;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Nosql\Select\DynamoSelect;
use Nosql\Adapter\AbstractDb;

/**
 * DynamoDb适配器
 */
class DynamoDb extends AbstractDb
{
    
    // 批量put操作
    const BATCH_REQUEST_PUT = "PutRequest";
    // 批量delete操作
    const BATCH_REQUEST_DELETE = "DeleteRequest ";
    protected $exec_time = 0;
    /**
     * Database client
     *
     * @var DynamoDbClient
     */
    protected $client = null;

    /**
     * 返回select对象
     *
     * @return DynamoSelect
     */
    public function select()
    {
        return new DynamoSelect ( $this );
    }

    /*
     * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::connect()
     */
    protected function connect()
    {
        if ($this->client) {
            return;
        }
        if ($this->checkRequiredConfig ()) {
            $Credential = new Credentials ( $this->config ['keyid'], $this->config ['access_key'] );
            $this->client = new DynamoDbClient ( array (
                'credentials' => $Credential,
                'version' => $this->config ['version'],
                'region' => $this->config ['region'] 
            ) );
        }
    }

    /**
     * 检查必须的配置
     *
     * @return boolean
     */
    private function checkRequiredConfig()
    {
        if (! key_exists ( "keyid", $this->config )) {
            return false;
        }
        if (! key_exists ( "access_key", $this->config )) {
            return false;
        }
        if (! key_exists ( "region", $this->config )) {
            return false;
        }
        if (! key_exists ( "version", $this->config )) {
            return false;
        }
        return true;
    }

    /*
     * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::closeConnection()
     */
    public function closeConnection()
    {
        // do nothing
    }

    /*
     * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::isConnected()
     */
    public function isConnected()
    {
        return $this->client != null;
    }

    /**
     * 获取表格信息
     */
    public function describeTable($tableName)
    {
        $this->connect ();
        return $this->client->describeTable ( array (
            "TableName" => $tableName 
        ) );
    }

    /**
     * 根据主键查询唯一一条item
     *
     * @param string $tableName
     *            表名
     * @param array $keyValue
     *            直接指定主键查询的条件
     *            例：array("key1"=>val1,"key2"=>val2)
     * @return array 返回的数据
     * @throws DynamoDbException
     */
    public function fetchOne($tableName, $keyValue)
    {
        return $this->getItem ( $tableName, $keyValue );
    }

    /**
     * query 查询
     * 
     * @param
     *            strin g$method 查询方式 可选 scan，query
     * @param array $ExclusiveStartKey
     *            启始位置
     * @param DynamoSelect $select            
     * @param boolean $isThrowException
     *            是否抛出异常，返回给前端时默认不抛出，执行后台脚本可抛出自行控制频率
     * @return array array(
     *         "Count"=>0,
     *         "Items"=>array()
     *         )
     */
    public function fetch($select, $limit = 0, $ExclusiveStartKey = null, $isThrowException = false)
    {
        $limit = intval ( $limit );
        $total = 0;
        $items = array ();
        $maxTryTimes = 6;
        // 没有指定limit时，直接会按dynamodb容量限制，可能每次取出的数量都不相同
        $figureLimit = 0;
        
        if ($this->exec_time && (time () - $this->exec_time) > 20) {
            // throw new \Exception("Maximum execution time of 30 seconds exceeded");
            $this->exec_time = 0;
            return array (
                "Items" => $items,
                "Count" => $total,
                "Skip" => null 
            );
        }
        
        if (! $ExclusiveStartKey) {
            if ($limit > 0) {
                $select->limit ( $limit );
            } else {
                $select->limit ( $figureLimit );
            }
            $query = $select->getQuery ();
            try {
                if ($query ["Method"] == DynamoSelect::$METHOD_QUERY) {
                    $moreResult = $this->query ( $query ["Params"] );
                } else {
                    $moreResult = $this->scan ( $query ["Params"] );
                }
                $ExclusiveStartKey = $moreResult ["Skip"];
                $total += $moreResult ["Count"];
                $items = array_merge ( $items, $moreResult ["Items"] );
            } catch ( \Exception $e ) {
                if ($isThrowException) {
                    throw new \Exception ( $e->getMessage (), $e->getCode () );
                }
                // file_put_contents("dynamodbError.log", "dynamodb error:". $e->getMessage());
                $ExclusiveStartKey = null;
                $maxTryTimes = 0;
            }
        }
        
        // 有下一页，且尝试次数小于限制，且 （查询结果小于limit或无limit）时，不断查下一页
        while ( $ExclusiveStartKey && $maxTryTimes > 0 && ($limit > 0 && $total < $limit || $limit == 0) ) {
            if ($this->exec_time && (time () - $this->exec_time) > 25) {
                // throw new \Exception("Maximum execution time of 30 seconds exceeded");
                $ExclusiveStartKey = null;
                $this->exec_time = 0;
                break;
            }
            
            $maxTryTimes --;
            if ($maxTryTimes % 2 == 0) {
                sleep ( 1 );
            } // 每3次停一秒，避免容量超频
            $select->setExclusiveStartKey ( $ExclusiveStartKey );
            if ($limit > 0) {
                // 有限制条数的，查询剩余条数
                $restLimit = $limit - $total;
                $select->limit ( $restLimit );
            } else {
                $select->limit ( $figureLimit );
            }
            $query = $select->getQuery ();
            try {
                if ($query ["Method"] == DynamoSelect::$METHOD_QUERY) {
                    $moreResult = $this->query ( $query ["Params"] );
                } else {
                    $moreResult = $this->scan ( $query ["Params"] );
                }
                $ExclusiveStartKey = $moreResult ["Skip"];
                $total += $moreResult ["Count"];
                $items = array_merge ( $items, $moreResult ["Items"] );
            } catch ( \Exception $e ) {
                if ($isThrowException) {
                    throw new \Exception ( $e->getMessage (), $e->getCode () );
                }
                // file_put_contents("dynamodbError.log", "dynamodb error:". $e->getMessage());
                $ExclusiveStartKey = null;
                break;
            }
        }
        return array (
            "Items" => $items,
            "Count" => $total,
            "Skip" => $ExclusiveStartKey 
        );
    }

    /**
     * 根据查询条件返回所有记录
     *
     * @param DynamoSelect $select            
     * @return array 成功返回 查询结果 ，false 无结果
     */
    public function fetchAll($select)
    {
        $this->exec_time = time ();
        $limit = $select->getLimit (); // 要获取的条数
        $skip = $select->getSkip (); // 要跳过的条数,$skip 必须是$limit的倍数
        if ($skip > 0) {
            // skip多页时，尽快的查询掉skip前的数据,直接limit查询skip条数据
            $skipResult = $this->fetch ( $select, $skip );
            $ExclusiveStartKey = $skipResult ["Skip"]; // 下一页的启始位置
            $skipCount = $skipResult ["Count"];
            if ($ExclusiveStartKey) {
                // skip后，还有下一页时，查询limit条数据
                $result = $this->fetch ( $select, $limit, $ExclusiveStartKey );
                $result ["Count"] = $result ["Count"] + $skipCount;
                if ($result ["Skip"]) {
                    // 如果有更多数据时，查total
                    $restResult = $this->fetch ( $select, 0, $result ["Skip"] );
                    $result ["Count"] = $result ["Count"] + $restResult ["Count"];
                }
                unset ( $result ["Skip"] );
                return $result;
            } else {
                // 从跳过的结果中取倒数$limit条
                return array (
                    "Items" => array_slice ( $skipResult ['Items'], 0 - $limit ),
                    "Count" => $skipCount 
                );
            }
        } else {
            // 不skip数据,查询limit条数据
            $result = $this->fetch ( $select, $limit );
            if ($result ["Skip"]) {
                // 如果有更多数据时，查total
                $restResult = $this->fetch ( $select, 0, $result ["Skip"] );
                $result ["Count"] = $result ["Count"] + $restResult ["Count"];
            }
            unset ( $result ["Skip"] );
            return $result;
        }
    }

    /**
     * 去掉数组的空元素
     */
    public function filterFields($data)
    {
        if (! is_array ( $data ))
            return $data;
        
        if (count ( $data ) == count ( $data, 1 )) {
            return array_filter ( $data );
        } else {
            foreach ( $data as $k => $a ) {
                $data [$k] = $this->filterFields ( $a );
            }
            return $data;
        }
    }

    /**
     * 保存或更新主键相同的item
     *
     * @param string $tableName
     *            表名
     * @param array $data
     *            要保存或更新的item
     * @return boolean true成功，false失败
     */
    public function save($tableName, $data)
    {
        // array_filter只能去一维数组的空值
        return $this->putItem ( $tableName, $this->filterFields ( $data ) );
    }

    /**
     * 更新一条item
     *
     * @param string $tableName
     *            表名
     * @param array $data
     *            要保存或更新的item
     * @param mixed $ids
     *            主键，如果是复合主键，要把所有主键都带上
     * @return boolean
     */
    public function update($tableName, $data, $ids)
    {
        return $this->updateItem ( $tableName, $this->filterFields ( $data ), $ids );
    }

    /**
     * 根据主键删除记录
     *
     * @param string $tableName
     *            表名
     * @param mixed $ids
     *            主键，如果是复合主键，要把所有主键都带上
     */
    public function delete($tableName, $ids)
    {
        return $this->deleteItem ( $tableName, $ids );
    }

    /**
     * 批量添加item
     *
     * @param string $tableName
     *            表名
     * @param array $items
     *            批量添加的item
     * @return boolean true 成功，false失败
     */
    public function batchSave($tableName, $items)
    {
        return $this->batchWriteItem ( $tableName, $this->filterFields ( $items ) );
    }

    /**
     * 批量删除item
     *
     * @param string $tableName
     *            表名
     * @param array $ids
     *            指定的主键
     * @return boolean true 成功，false失败
     */
    public function batchDelete($tableName, $ids)
    {
        return $this->batchWriteItem ( $tableName, $ids, self::BATCH_REQUEST_DELETE );
    }

    // ///////DynamoDb提供的接口操作
    private function batchGetItem()
    {
        // TODO
    }

    /**
     * 批量写入，最多写入25条记录
     *
     * @param string $tableName
     *            表名
     * @param array $datas
     *            批量添加的数据或删除指定的ids
     * @param string $type
     *            批量操作类型 self::BATCH_REQUEST_*
     */
    private function batchWriteItem($tableName, $datas, $type = self::BATCH_REQUEST_PUT)
    {
        $count = 0;
        $RequestItem = array ();
        $params = array ();
        $marshaler = new Marshaler ();
        foreach ( $datas as $k => $item ) {
            $item = $marshaler->marshalItem ( $item );
            $data = array ();
            if ($type == self::BATCH_REQUEST_PUT) {
                $data [$type] ["Item"] = $item;
            } else {
                $data [$type] ["Key"] = $item;
            }
            $RequestItem [$tableName] [] = $data;
            $count ++;
            if ($count >= 25) {
                $params ["RequestItems"] = $RequestItem;
                $this->connect ();
                $rs = $this->client->batchWriteItem ( $params );
                $metadta = $rs->get ( "@metadata" );
                if ($metadta && $metadta ["statusCode"] != 200) {
                    return false;
                }
                unset ( $RequestItem );
                $RequestItem = array ();
            }
        }
        if (! empty ( $RequestItem )) {
            $this->connect ();
            $params ["RequestItems"] = $RequestItem;
            $rs = $this->client->batchWriteItem ( $params );
        }
        if ($rs) {
            $metadta = $rs->get ( "@metadata" );
            if ($metadta && $metadta ["statusCode"] == 200) {
                return true;
            }
        }
        return false;
    }

    /**
     * 删除item
     * 
     * @param string $tableName            
     * @param array $keyValues            
     */
    private function deleteItem($tableName, $keyValues)
    {
        $marshaler = new Marshaler ();
        $params = array (
            "TableName" => $tableName,
            "Key" => $marshaler->marshalItem ( $keyValues ) 
        );
        $this->connect ();
        $rs = $this->client->deleteItem ( $params );
        $metadta = $rs->get ( "@metadata" );
        if ($metadta && $metadta ["statusCode"] == 200) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 根据主键查询单条item
     *
     * @param array $keyValue
     *            直接指定主键查询的条件
     * @return array NULL
     * @throws
     *
     */
    private function getItem($tableName, $keyValue)
    {
        $marshaler = new Marshaler ();
        $params = array (
            "TableName" => $tableName,
            "ConsistentRead" => true,
            "Key" => $marshaler->marshalItem ( $keyValue ) 
        );
        $this->connect ();
        $result = $this->client->getItem ( $params );
        if ($result ["Item"]) {
            $item = $marshaler->unmarshalItem ( $result ["Item"] );
            return $item;
        } else {
            return null;
        }
    }

    /**
     * 保存新item或根据条件覆盖旧item
     *
     * @param string $tableName
     *            表名
     * @param array $data
     *            要保存的item
     * @param
     *            array cond 如果传递该参数，只有满足条件时item才会保存成功
     */
    private function putItem($tableName, $data, $cond = null)
    {
        $this->connect ();
        $mash = new Marshaler ();
        $params ["Item"] = $mash->marshalItem ( $data );
        $params ["TableName"] = $tableName;
        $rs = $this->client->putItem ( $params );
        $metadta = $rs->get ( "@metadata" );
        if ($metadta && $metadta ["statusCode"] == 200) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 查询主键
     *
     * @param array $params
     *            调接口的参数
     * @return boolean array "Items"=>array(),//返回的数据
     *         "Count"=>1,//返回的数据条目
     *         "ScannedCount"=>1,//扫描的条数，用于判断查询的效率及优化，但ScannedCount跟Count差距很大时，说明效率低下。
     *         )
     * @throws Exception
     */
    private function query($params)
    {
        $this->connect ();
        $rs = $this->client->query ( $params );
        $metadta = $rs->get ( "@metadata" );
        if ($metadta && $metadta ["statusCode"] == 200) {
            $items = $rs->get ( "Items" );
            $items = $this->unmashItems ( $items );
            return array (
                "Items" => $items,
                "Count" => $rs->get ( "Count" ),
                "Skip" => $rs->get ( "LastEvaluatedKey" ),
                "ScannedCount" => $rs->get ( "ScannedCount" ) 
            );
        } else {
            var_dump($metadta);
            return false;
        }
    }

    /**
     * unmash items
     *
     * @param array $items            
     */
    private function unmashItems(&$items)
    {
        $mash = new Marshaler ();
        foreach ( $items as $key => $item ) {
            $items [$key] = $mash->unmarshalItem ( $item );
        }
        return $items;
    }

    /**
     * 按非主键查询
     *
     * @param array $params            
     * @return boolean
     */
    private function scan($params)
    {
        $this->connect ();
        $rs = $this->client->scan ( $params );
        $metadta = $rs->get ( "@metadata" );
        if ($metadta && $metadta ["statusCode"] == 200) {
            $items = $rs->get ( "Items" );
            $items = $this->unmashItems ( $items );
            return array (
                "Items" => $items,
                "Skip" => $rs->get ( "LastEvaluatedKey" ),
                "ScannedCount" => $rs->get ( "ScannedCount" ),
                "Count" => $rs->get ( "Count" ) 
            );
        } else {
            return false;
        }
    }

    /**
     * 更新或增加一条新item
     *
     * @param string $tableName            
     * @param array $data
     *            要更新的属性
     * @param array $keys
     *            指定id
     */
    private function updateItem($tableName, $data, $keys)
    {
        $mash = new Marshaler ();
        $params ["Key"] = $mash->marshalItem ( $keys );
        $params ["TableName"] = $tableName;
        $updateExpression = array ();
        $ExpressionAttributeNames = array ();
        $index = 0;
        $bind = array ();
        foreach ( $data as $k => $v ) {
            if (key_exists ( $k, $keys )) {
                // 主键是不允许更新的
                continue;
            }
            $key = ":v_" . $index;
            $ExpressionAttributeNames ["#" . $k] = $k;
            $updateExpression [] = "#" . $k . "=" . $key;
            $bind [$key] = $v;
            $index ++;
        }
        if (! empty ( $updateExpression )) {
            $params ["UpdateExpression"] = " set " . join ( ",", $updateExpression );
        }
        if (! empty ( $ExpressionAttributeNames )) {
            $params ["ExpressionAttributeNames"] = $ExpressionAttributeNames;
        }
        if (! empty ( $bind )) {
            $params ["ExpressionAttributeValues"] = $mash->marshalItem ( $bind );
        }
        $this->connect ();
        $rs = $this->client->updateItem ( $params );
        $metadta = $rs->get ( "@metadata" );
        if ($metadta && $metadta ["statusCode"] == 200) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 转义字符
     *
     * @param string $value            
     * @return unknown string
     */
    public function quoteStr($value)
    {
        if (is_int ( $value )) {
            return $value;
        } elseif (is_float ( $value )) {
            return sprintf ( '%F', $value );
        }
        return "'" . addcslashes ( $value, "\000\n\r\\'\"\032" ) . "'";
    }

    /**
     * 生成一个条件字符串，通常用于and中的or操作。如： expression1 and (expression2 or expression3)
     *
     * @param string $cond
     *            查询表达式
     *            例：Field1=:p1: And Field1!=:p2: 其中p1，跟p2就是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("p1"=>"value1","p2"=>"value2")
     */
    public function quote($cond, $bind){
		foreach ($bind as $key=>$v){
			$v = $this->quoteStr($v);
			$cond = preg_replace("/:".$key.":/", $v, $cond);
		}
		return $cond;
	}
}