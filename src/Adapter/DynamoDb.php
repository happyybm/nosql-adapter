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
 * 没有转议功能，在使用时要注意关键字冲突问题。
 */
class DynamoDb extends AbstractDb {
	
	/**
	 * 返回select对象
	 *
	 * @return DynamoSelect
	 */
	public function select() {
		return new DynamoSelect ( $this );
	}
	
	/*
	 * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::_connect()
	 */
	protected function _connect() {
		if ($this->_connection) {
			return;
		}
		if ($this->checkRequiredConfig ()) {
			$Credential = new Credentials ( $this->_config ['keyid'], $this->_config ['access_key'] );
			$this->_connection = new DynamoDbClient ( array (
					'credentials' => $Credential,
					'version' => $this->_config ['version'],
					'region' => $this->_config ['region'] 
			) );
		}
	}
	/**
	 * 检查必须的配置
	 *
	 * @return boolean
	 */
	private function checkRequiredConfig() {
		if (! key_exists ( "keyid", $this->_config )) {
			return false;
		}
		if (! key_exists ( "access_key", $this->_config )) {
			return false;
		}
		if (! key_exists ( "region", $this->_config )) {
			return false;
		}
		if (! key_exists ( "version", $this->_config )) {
			return false;
		}
		return true;
	}
	
	/*
	 * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::closeConnection()
	 */
	public function closeConnection() {
		// do nothing
	}
	
	/*
	 * (non-PHPdoc) @see \Duomai\Db\Adapter\AbstractDb::isConnected()
	 */
	public function isConnected() {
		return $this->_connection != null;
	}
	
	/**
	 * (non-PHPdoc)
	 *
	 * @see \Duomai\Db\Adapter\AbstractDb::describeTable()
	 */
	public function describeTable($tableName) {
		$this->_connect ();
		return $this->_connection->describeTable ( array (
				"TableName" => $tableName 
		) );
	}
	
	/**
	 * 根据主键查询唯一一条item
	 * 
	 * @param string $tableName
	 *        	表名
	 * @param array $keyValue
	 *        	直接指定主键查询的条件
	 *        	例：array("key1"=>val1,"key2"=>val2)
	 * @return array 返回的数据
	 * @throws DynamoDbException
	 */
	public function fetchOne($tableName, $keyValue) {
		return $this->getItem ( $tableName, $keyValue );
	}
	
	/**
	 * 根据查询条件返回所有记录
	 * 
	 * @param DynamoSelect $select        	
	 */
	public function fetchAll($select) {
		$query = $select->getQuery ();
		print_r ( $query );
		if ($query ["Method"] == DynamoSelect::$METHOD_QUERY) {
			return $this->query ( $query ["Params"] );
		} else {
			return $this->scan ( $query ["Params"] );
		}
	}
	
	/**
	 * 保存或更新主键相同的item
	 * 
	 * @param string $tableName
	 *        	表名
	 * @param array $data
	 *        	要保存或更新的item
	 */
	public function save($tableName, $data) {
		return $this->putItem ( $tableName, $data );
	}
	
	/**
	 * 更新一条item
	 * 
	 * @param string $tableName
	 *        	表名
	 * @param array $data
	 *        	要保存或更新的item
	 * @param array $ids
	 *        	主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
	 * @return boolean
	 */
	public function update($tableName, $data, $ids) {
		return $this->updateItem ( $tableName, $data, $ids );
	}
	
	/**
	 * 根据主键删除记录
	 * 
	 * @param string $tableName
	 *        	表名
	 * @param array $ids
	 *        	主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
	 */
	public function delete($tableName, $ids) {
		return $this->deleteItem ( $tableName, $ids );
	}
	
	// ///////DynamoDb提供的接口操作
	private function batchGetItem() {
		// TODO
	}
	private function batchWriteItem() {
		// TODO
	}
	/**
	 *删除item
	 * @param string $tableName        	
	 * @param array $keyValues        	
	 */
	private function deleteItem($tableName, $keyValues) {
		$marshaler = new Marshaler ();
		$params = array (
				"TableName" => $tableName,
				"Key" => $marshaler->marshalItem ( $keyValues ) 
		);
		$this->_connect ();
		$rs = $this->_connection->deleteItem ( $params );
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
	 *        	直接指定主键查询的条件
	 * @return array NULL
	 * @throws
	 *
	 */
	private function getItem($tableName, $keyValue) {
		$marshaler = new Marshaler ();
		$params = array (
				"TableName" => $tableName,
				"ConsistentRead" => true,
				"Key" => $marshaler->marshalItem ( $keyValue ) 
		);
		$this->_connect ();
		$result = $this->_connection->getItem ( $params );
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
	 *        	表名
	 * @param array $data
	 *        	要保存的item
	 * @param
	 *        	array cond 如果传递该参数，只有满足条件时item才会保存成功
	 */
	private function putItem($tableName, $data, $cond = null) {
		$this->_connect ();
		$mash = new Marshaler ();
		$params ["Item"] = $mash->marshalItem ( $data );
		$params ["TableName"] = $tableName;
		$rs = $this->_connection->putItem ( $params );
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
	 *        	调接口的参数
	 * @return boolean array "Items"=>array(),//返回的数据
	 *         "Count"=>1,//返回的数据条目
	 *         "ScannedCount"=>1,//扫描的条数，用于判断查询的效率及优化，但ScannedCount跟Count差距很大时，说明效率低下。
	 *         )
	 */
	private function query($params) {
		// TODO 考虑分页的问题
		$rs = $this->_connection->query ( $params );
		$metadta = $rs->get ( "@metadata" );
		if ($metadta && $metadta ["statusCode"] == 200) {
			$items = $rs->get ( "Items" );
			$count = $rs->get ( "Count" );
			$ScannedCount = $rs->get ( "ScannedCount" );
			$items = $this->unmashItems ( $items );
			return array (
					"Items" => $items,
					"Count" => $count,
					"ScannedCount" => $ScannedCount 
			);
		} else {
			return false;
		}
	}
	
	/**
	 * unmash items
	 * 
	 * @param array $items        	
	 */
	private function unmashItems(&$items) {
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
	private function scan($params) {
		// TODO 考虑分页的问题
		$rs = $this->_connection->scan ( $params );
		$metadta = $rs->get ( "@metadata" );
		if ($metadta && $metadta ["statusCode"] == 200) {
			$items = $rs->get ( "Items" );
			$count = $rs->get ( "Count" );
			$items = $this->unmashItems ( $items );
			return array (
					"Items" => $items,
					"Count" => $count 
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
	 *        	要更新的属性
	 * @param array $keys
	 *        	指定id
	 */
	private function updateItem($tableName, $data, $keys) {
		$mash = new Marshaler ();
		$params ["Key"] = $mash->marshalItem ( $keys );
		$params ["TableName"] = $tableName;
		$updateExpression = array ();
		$index = 0;
		$bind = array ();
		foreach ( $data as $k => $v ) {
			if (key_exists ( $k, $keys )) {
				// 主键是不允许更新的
				continue;
			}
			$key = ":v_" . $index;
			$updateExpression [] = $k . "=" . $key;
			$bind [$key] = $v;
			$index ++;
		}
		if (! empty ( $updateExpression )) {
			$params ["UpdateExpression"] = " set " . join ( ",", $updateExpression );
		}
		if (! empty ( $bind )) {
			$params ["ExpressionAttributeValues"] = $mash->marshalItem ( $bind );
		}
		$this->_connect ();
		$rs = $this->_connection->updateItem ( $params );
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
	public function quoteStr($value) {
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
	 *        	查询表达式
	 *        	例：Session=:param1 And Session<>:param2 其中param1，跟param2就是条件占位符，在参数bind中指定其值。
	 * @param array $bind
	 *        	绑定的参数,由cond中的参数占位符指定
	 *        	例：array("param1"=>"sdfsdfsd","param2"=>"sdfsdfsdf")
	 */
	public function quote($cond, $bind){
		foreach ($bind as $key=>$v){
			$v = $this->quoteStr($v);
			$cond = preg_replace("/:".$key."/", $v, $cond);
		}
		return $cond;
	}
}