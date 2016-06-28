<?php
namespace Nosql\Select;
use Nosql\DbException;
use Nosql\Adapter\AbstractDb;
/**
 * 组织转义DynamoDB的查询条件
 */
abstract class Select{
	/**
	 * 错误信息记录
	 * @var string
	 */
	public $errors;
	/**
	 * 表名
	 * @var string
	 */
	public $tableName;
	/**
	 * 属性的映射定义，用于转义
	 * @var array
	 */
	public $attrMapping;
	
	/**
	 * 要返回的属性
	 * @var mixed
	 */
	public $colums = "*";
	
	//没有having，没有group，
	/**
	 * and操作记录
	 * @var array
	 */
	protected $ands = array();
	/**
	 * or操作记录
	 * @var array
	 * 结构如：
	 * array(
	 * 		array(
	 * 			"Op"=>"=",
	 * 			"Field"=>"domain_name",
	 * 			"Ps"=>"dn"
	 * 		),
	 * )
	 */
	protected $ors = array();
	/**
	 * 绑定参数
	 * @var array
	 */
	protected $binds = array();
	
	/**
	 * 排序操作记录
	 * @var array
	 */
	protected $orders = array();
	
	/**
	 * 一次查询大小
	 * @var int
	 */
	protected $limit = 20;
	
	/**
	 * 跳过的条数
	 * @var int
	 */
	protected $skip = 0;
	
	/**
	 * 数据库适配器
	 * @var AbstractDb
	 */
	protected $adapter;
	
	/**
	 * 构造函数
	 * @param AbstractDb $adapter
	 */
	public function __construct(AbstractDb $adapter)
	{
		$this->adapter = $adapter;
	}
	
	/**
	 * 返回查询的表名
	 * @return string
	 */
	public function getTableName(){
		return $this->tableName;
	}
	
	/**
	 * @return the $attrMapping
	 */
	public function getAttrMapping() {
		return $this->attrMapping;
	}

	/**
	 * @return the $colums
	 */
	public function getColums() {
		return $this->colums;
	}

	/**
	 * @return the $adapter
	 */
	public function getAdapter() {
		return $this->adapter;
	}

	/**
	 * 从哪个表里查数据
	 * @param string $tableName
	 * @return Select
	 */
	abstract public function from($tableName);
	/**
	 * 需要返回的属性
	 * @param string|Array $columns *表示查所有属性 
	 * @return Select
	 */
	public function columns($columns="*"){
		$this->colums = $columns;
		return $this;
	}
	/**
	 * 设置属性转义
	 * 例：Session跟服务的关键词冲突，则需要定义array("Session","mySession");这种方式，意思是把Session用别名mySession代替
	 * @param array $attrMapping
	 * @return Select
	 */
	public function setAttrMapping($attrMapping){
		$this->attrMapping = $attrMapping;
		return $this;
	}
	/**
	 * 查询条件，and查询
	 * @param string $cond 查询表达式
	 * 例：Session=:param1 And Session<>:param2 其中param1，跟param2就是条件占位符，在参数bind中指定其值。
	 * @param array $bind 绑定的参数,由cond中的参数占位符指定
	 * 例：array("param1"=>"sdfsdfsd","param2"=>"sdfsdfsdf")
	 * @return Select
	 */
	public function where($cond,$bind){
		$this->ands[] = $cond;
		$this->binds = array_merge($this->binds,$bind);
		return $this;
	}
	/**
	 * 查询条件 or查询，
	 * @param string $cond 查询表达式
	 * 例：Session=:param1 And Session<>:param2 其中param1，跟param2就是条件占位符，在参数bind中指定其值。
	 * @param array $bind 绑定的参数,由cond中的参数占位符指定
	 * 例：array("param1"=>"sdfsdfsd","param2"=>"sdfsdfsdf")
	 * @return Select
	 */
	public function orWhere($cond,$bind){
		$this->ors[] =$cond;
		$this->binds = array_merge($this->binds,$bind);
		return $this;
	}
	/**
	 * and 的 in 操作
	 * @param unknown $field
	 * @param unknown $values
	 * @return Select
	 */
	public function inWhere($field,$values){
		$alias = "in_".$field;
		$cond = $field ." in (:".$alias.")";
		$isStr=false;//默认都按字符串处理
		foreach ($values as $v){
			if(is_int($v)||is_float($v)||is_bool($v)){
				continue;
			}
			$isStr = true;
		}
		if($isStr){
			$bind = array($alias=>join(",", $values));
		}else{
			foreach ($values as $k=>$v){
				$v = "'".$this->getAdapter()->quoteStr($v)."'";
				$values[$k] = $v;
			}
		}
		$this->ands[] = preg_replace("/:".$alias."/", "(".join(",", $values).")", $cond);
		return $this;
	}
	
	/**
	 * 排序操作
	 * 例：array("ItemID"=>"Desc","CreateTime"=>"ASC");
	 * @param array $order
	 * @return Select
	 */
	public function orderBy($order){
		$this->orders = array_merge($this->orders,$order);
		return $this;
	}
	
	/**
	 * 限制查询条数
	 * @param int $limit
	 * @param mixed $skip 跳过的条数，或起始数据
	 * @return Select
	 */
	public function limit($limit,$skip=null){
		$this->limit = $limit;
		$this->skip = $skip;
		return $this;
	}
	
	/**
	 * 附加错误信息
	 * @param unknown $msg
	 */
	public function error($msg){
		$this->errors.=" ".$msg;
	}
	
	/**
	 * 查询前的检查操作
	 */
	protected abstract function checkCond();
	/**
	 * 根据设置的条件生成具体数据库适配的查询格式 
	 * @return mixed 不同数据库的格式不一样
	 */
	protected abstract function assemble();
	
	/**
	 * 返回直接用于接口的查询的参数格式
	 * @throws DbException
	 * @return mixed 不同数据库的格式不一样
	 */
	public function getQuery(){
		if($this->checkCond()){
			return $this->assemble();
		}else{
			throw new DbException($this->errors);
		}
	}
}