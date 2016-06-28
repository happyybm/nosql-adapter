<?php

namespace Nosql\Select;

use Nosql\Select\Select;
use Nosql\DbException;
use Aws\DynamoDb\Marshaler;

class DynamoSelect extends Select {
	
	static $METHOD_QUERY = "Query";
	static $METHOD_SCAN = "Scan";
	
	const COND_AND = "AND";
	const COND_OR = "OR";
	
	//支持的查询操作
	const OP_EQ = "=";
	const OP_NEQ = "<>";
	const OP_LT = "<";
	const OP_LTEQ = "<=";
	const OP_GT = ">";
	const OP_GTEQ = ">=";
	const OP_LIKE = "LIKE";
	const OP_IS_NULL = "IS NULL";
	const OP_IS_NOT_NULL = "IS NOT NULL";
	const OP_BETWEEN = "BETWEEN";

	private $keyCond = array();
	private $andCond = array();
	private $orCond=array();
	private $filter;
	private $indexName;
	
	/**
	 * 表信息：属性，主键，索引
	 * 分区键查询只能适用=操作
	 * @var array
	 */
	private $tableInfo = array (
			"Fields" => array (),
			"Keys" => array (),
			"Indexs" => array () 
	);
	
	
	
	/* (non-PHPdoc)
	 * @see \Duomai\Db\Select\Select::from()
	 */
	public function from($tableName) {
		if(empty($this->tableName)){
			$this->tableName = $tableName;
		}else{
			$this->indexName = $tableName;
		}
		return $this;
	}

	
	
	/**
	 * 根据不同的条件，指定使用的查询方式：Query,Scan
	 * 通用条件
	 * TableName 表名
	 * ConsistentRead 是否强一致性读取，取一条数据时，要求强一致性？
	 * ProjectionExpression 要返回的属性
	 * ExpressionAttributeNames 属性别名 如{"#P":"Percentile"},可指定多个
	 * ExpressionAttributeValues 属性值：如：{ ":avail":{"S":"Available"}, ":back":{"S":"Backordered"}, ":disc":{"S":"Discontinued"} }
	 *
	 * /////Query 按主键查询多条item
	 * Limit 要返回的条数，结合ExclusiveStartKey提供翻页效果
	 * ExclusiveStartKey 启始索引，当查询超过吞吐量时，要经过多次查询才能返回整个结果。
	 * ScanIndexForward 排序操作 true 正序，false：倒序，是按排序索引排的
	 * IndexName 指定查询的索引
	 * KeyConditionExpression 只能是主键的查询条件,如：partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
	 * FilterExpression 只能是非索引的属性的过滤，过滤操作是要查询结果后再操作的，所以即使过滤这些item同样也是耗费资源的
	 *
	 * ///Scan 非按主键的查询,scan时没有排序操作，排序只能对索引进行排序
	 * Limit 要返回的条数，结合ExclusiveStartKey提供翻页效果
	 * ExclusiveStartKey 启始索引，当查询超过吞吐量时，要经过多次查询才能返回整个结果。
	 * FilterExpression 只能是非索引的属性的过滤，过滤操作是要查询结果后再操作的，所以即使过滤这些item同样也是耗费资源的
	 * Segment 并行查询时指定的进程号 默认从0开始，如果结合ExclusiveStartKey的话，这个Segment也要跟上，这样分页才对。
	 * TotalSegments 指定并行查询的进程数，1～1000000之间 跟参数 Segment一起用
	 * 
	 * @return array $query
	 *         例：
	 *         array(
	 *         "Method"=>"Query",//需要调用的接口
	 *         "Params"=>array()//接口参数
	 *         )
	 */
	protected function assemble() {
		$marsh = new Marshaler ();
		// 设置表名,默认非强一致性
		$params = array (
				"TableName" => $this->getTableName (),
				"ConsistentRead" => false 
		);
		
		$ProjectionExpression = array ();
		$ExpressionAttributeNames = array ();
		$ExpressionAttributeValues = array ();
		
		if (! empty ( $this->colums )&&$this->colums!="*") {
			if (! empty ( $this->attrMapping )) {
				// 别名替换
				foreach ( $this->colums as $k => $field ) {
					if (key_exists ( $field, $this->attrMapping )) {
						$this->colums [$k] = "#" . $this->attrMapping [$field];
					}
				}
			}
			$params ["ProjectionExpression"] = join ( ",", $this->colums );
		}
		
		if (! empty ( $this->getAttrMapping () )) {
			foreach ( $this->getAttrMapping () as $field => $mapKey ) {
				$ExpressionAttributeNames ["#" . $mapKey] = $field;
			}
			$params ["ExpressionAttributeNames"] = $ExpressionAttributeNames;
		}
		if (! empty ( $this->binds )) {
			foreach ( $this->binds as $k => $v ) {
				$ExpressionAttributeValues [":" . $k] = $v;
			}
			$json = json_encode ( $ExpressionAttributeValues );
			$params ["ExpressionAttributeValues"] = $marsh->marshalJson ( $json );
		}
		if($this->limit>0){
			$params["limit"] = $this->limit;
		}
		if(!empty($this->skip)){
			$params["ExclusiveStartKey"] = $this->skip;
		}
		if(!empty($this->orders)){
			//TODO 只能处理正序，倒序，且在有排序键的前提下
			$order = "asc";
			foreach ($this->orders as $file=>$o){
				$order = strtolower($o);
				break;
			}
			$params["ScanIndexForward"]=($order=="asc")?true:false;
		}
		if($this->indexName){
			$params["IndexName"] = $this->indexName;
		}
		foreach ($this->ands as $expression){
			$this->explorCond($expression,self::COND_AND);
		}
		foreach ($this->ors as $expression){
			$this->explorCond($expression,self::COND_OR);
		}
		if(!empty($this->filter)){
			$params["FilterExpression"] = join(" and ", $this->filter);
		}
		// 没有Keys，Index中的查询，需要采用scan操作，否则使用query操作。非key，index中的条件，用FilterExpression处理。
		$KeyConditionExpression = "";
		foreach ($this->andCond as $cond){
			$field= $cond["Field"];
			$cond = join(" ",$cond);
			if(in_array($field, $this->tableInfo["Keys"])||in_array($field, $this->tableInfo["Indexs"])){
				if($KeyConditionExpression==""){
					$KeyConditionExpression= $cond;
				}else{
					$KeyConditionExpression.=" and ". $cond;
				}
			}else{
				if(!empty($params["FilterExpression"])){
					$params["FilterExpression"].=" and ".$cond;
				}else{
					$params["FilterExpression"]=$cond;
				}
			}
		}
		foreach ($this->orCond as $cond){
			$field= $cond["Field"];
			$cond = join(" ",$cond);
			if(in_array($field, $this->tableInfo["Keys"])){
				if($KeyConditionExpression==""){
					$KeyConditionExpression= $cond;
				}else{
					$KeyConditionExpression.=" or ". $cond;
				}
			}else{
				if(!empty($params["FilterExpression"])){
					$params["FilterExpression"].=" or ".$cond;
				}else{
					$params["FilterExpression"]=$cond;
				}
			}
		}
		
		if (empty ( $KeyConditionExpression )) {
			$result ["Method"] = self::$METHOD_SCAN;
		} else {
			$params["KeyConditionExpression"]=$KeyConditionExpression;
			$result ["Method"] = self::$METHOD_QUERY;
		}
		$result ["Params"] = $params;
		return $result;
	}
	/**
	 * 按优先级拆分查询语句,嵌套的or操作只能filter处理
	 * @param string $expression 语句
	 * @param string $cond and 或or
	 * @return array  单个语句的数组组合
	 */
	private function explorCond($expression,$cond=self::COND_AND){
		$expression = trim($expression);
		if($expression){
			
			if(preg_match("/^\(.*\)$/",$expression)){
				//有括号的放入filter
				$this->filter[] = $expression;
			}
			
			$strupper = strtoupper($expression);
			$opPreg = array(
					self::OP_BETWEEN,
					self::OP_EQ,
					self::OP_GT,
					self::OP_GTEQ,
					self::OP_IS_NOT_NULL,
					self::OP_IS_NULL,
					self::OP_LIKE,
					self::OP_LT,
					self::OP_LTEQ,
					self::OP_NEQ,
			);
			$opPreg = "(".join(")(", $opPreg).")";
			//按优先级，按and先分
			if(strpos($strupper, " ".self::COND_AND." ")>0){
				$pos = strpos($strupper, " ".self::COND_AND." ");
				$left = substr($expression, 0,$pos);
				$this->explorCond($left,self::COND_AND);
				$right = substr($expression, $pos+3);
				$this->explorCond($right,self::COND_AND);
			}elseif(strpos($strupper, " ".self::COND_OR." ")>0){
				$pos = strpos($strupper, " ".self::COND_OR." ");
				$left = substr($expression, 0,$pos);
				$this->explorCond($left,self::COND_OR);
				$right = substr($expression, $pos+2);
				$this->explorCond($right,self::COND_OR);
			}elseif(preg_match("/^(?<field>[\w\d_]+)(?<op>[".$opPreg."])(?<val>:?[\w\d_]+)$/i", $expression,$match)){
				if($cond==self::COND_OR){
					$this->orCond[] = array(
							"Field"=>$match["field"],
							"Op"=>$match["op"],
							"Value"=>$match["val"],
					);
				}elseif($cond==self::COND_AND){
					$this->andCond[] = array(
							"Field"=>$match["field"],
							"Op"=>$match["op"],
							"Value"=>$match["val"],
					);
				}
				return;
			}else{
				//放入filter
				$this->filter[] = $expression;
			}
		}else{
			throw new DbException("invalie condition");
		}
	}
	
	/*
	 * (non-PHPdoc) @see \Duomai\Db\Select\Select::checkCond()
	 */
	protected function checkCond() {
		// describe table 获取表信息
		$result = $this->adapter->describeTable ( $this->tableName );
		// 所有属性名称
		foreach ( $result ["Table"] ["AttributeDefinitions"] as $attr ) {
			$this->tableInfo ["Fields"] [] = $attr ["AttributeName"];
		}
		// 主键名称
		foreach ( $result ["Table"] ["KeySchema"] as $keys ) {
			$this->tableInfo ["Keys"] [] = $keys ["AttributeName"];
		}
		// 全局索引名称
		if(isset($result ["Table"] ["GlobalSecondaryIndexes"])){
			foreach ( $result ["Table"] ["GlobalSecondaryIndexes"] as $keys ) {
				foreach ( $keys ["KeySchema"] as $key ) {
					$this->tableInfo ["Indexs"] [] = $key ["AttributeName"];
				}
			}
		}
		// 本地索引名称
		if(isset($result ["Table"] ["LocalSecondaryIndexes"])){
			foreach ( $result ["Table"] ["LocalSecondaryIndexes"] as $keys ) {
				foreach ( $keys ["KeySchema"] as $key ) {
					$this->tableInfo ["Indexs"] [] = $key ["AttributeName"];
				}
			}
		}
		// 2.检查ands，ors，columns，limit，order中的属性是否正确
		return  $this->checkAnds () && $this->checkOrs () && $this->checkOrders ();
	}
	
	/**
	 * 检查ands是否正确
	 */
	protected function checkAnds() {
		//TODO
		return true;
	}
	protected function checkOrs() {
		//TODO
		return true;
	}
	protected function checkOrders() {
		//TODO
		return true;
	}
}