<?php
/**
 * nosql 的适配操作
 */
namespace Nosql\Adapter;
use Nosql\Select\Select;
abstract class AbstractDb {
	/**
	 * User-provided configuration
	 *
	 * @var array
	 */
	protected $_config = array ();
	
	/**
	 * Database connection
	 *
	 * @var DynamoDbClient
	 */
	protected $_connection = null;
	
	/**
	 *
	 * @param array $config        	
	 */
	public function __construct($config) {
		$this->_config = $config;
	}
	/**
	 * Returns the configuration variables in this adapter.
	 *
	 * @return array
	 */
	public function getConfig() {
		return $this->_config;
	}
	
	/**
	 * 获取连接
	 *
	 * @return object resource null
	 */
	public function getConnection() {
		$this->_connect ();
		return $this->_connection;
	}
	
	/**
	 *  返回select对象
	 *  @return Select
	 */
	abstract public function select();
	
	/**
	 * 连接到数据库的方法，具体由不同的适配器自己处理
	 *
	 * @return void
	 */
	abstract protected function _connect();
	/**
	 * 判断连接是否正常
	 *
	 * @return boolean
	 */
	abstract public function isConnected();
	/**
	 * 关闭连接
	 *
	 * @return void
	 */
	abstract public function closeConnection();
	
	/**
	 * describe table
	 * @param string $tableName 表名
	 */
	abstract public function describeTable($tableName);
	
	/**
	 * 保存或更新有相同主键的记录
	 * @param string $tableName 表名
	 * @param array $data 数据
	 */
	abstract public function save($tableName,$data);
	
	/**
	 * 更新一条item
	 * @param string $tableName 表名
	 * @param array $data 要保存或更新的item
	 * @param array $ids 主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
	 * @return boolean
	 */
	abstract public function update($tableName,$data,$ids);
	
	/**
	 * 根据主键删除记录
	 * @param string $tableName 表名
	 * @param array $ids 主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
	 */
	abstract public function delete($tableName,$ids);
	
	//item的相关操作
	/**
	 * 返回一条记录
	 * @param string $tableName
	 * @param array $keyValue
	 */
	abstract public function fetchOne($tableName,$keyValue);
	
	/**
	 * 根据select条件查询所有记录
	 * @param Select $select
	 */
	abstract public function fetchAll($select);
	
	/**
	 * 生成一个条件字符串，通常用于and中的or操作。如： expression1 and (expression2 or expression3)
	 * @param string $cond 查询表达式
	 * 例：Session=:param1 And Session<>:param2 其中param1，跟param2就是条件占位符，在参数bind中指定其值。
	 * @param array $bind 绑定的参数,由cond中的参数占位符指定
	 * 例：array("param1"=>"sdfsdfsd","param2"=>"sdfsdfsdf")
	 */
	abstract public function quote($cond,$bind);
	
	/**
	 * 转义字符
	 * @param string $value
	 * @return string
	*/
	abstract public  function quoteStr($value);
}
