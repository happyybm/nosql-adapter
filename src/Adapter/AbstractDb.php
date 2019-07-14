<?php
namespace Nosql\Adapter;

use Nosql\Select\Select;

/**
 * nosql 的适配
 */
abstract class AbstractDb
{
    /**
     * 配置信息
     *
     * @var array
     */
    protected $config = array ();

    /**
     *
     * @param array $config            
     */
    public function __construct($config)
    {
        $this->config = $config;
    }

    /**
     * 返回配置信息
     *
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * 获取连接
     *
     * @return object resource null
     */
    public function getConnection()
    {
        $this->_connect ();
        return $this->_connection;
    }

    /**
     * 返回select对象
     *
     * @return Select
     */
    abstract public function select();

    /**
     * 连接到数据库的方法，具体由不同的适配器自己处理
     *
     * @return void
     */
    abstract protected function connect();

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
     * 保存或更新有相同主键的记录
     *
     * @param string $tableName
     *            表名
     * @param array $data
     *            数据
     */
    abstract public function save($tableName, $data);

    /**
     * 批量保存item
     * 
     * @param string $tableName
     *            表名
     * @param array $items
     *            array
     */
    abstract public function batchSave($tableName, $items);

    /**
     * 批量删除记录
     * 
     * @param string $tableName
     *            表名
     * @param array $ids
     *            指定删除的主键数组
     */
    abstract public function batchDelete($tableName, $ids);

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
    abstract public function update($tableName, $data, $ids);

    /**
     * 根据主键删除记录
     *
     * @param string $tableName
     *            表名
     * @param mixed $primaryKeys
     *            主键信息，如果是复合主键，要把所有主键都带上
     */
    abstract public function delete($tableName, $primaryKeys);

    // item的相关操作
    /**
     * 返回一条记录
     *
     * @param string $tableName
     *            表名
     * @param mixed $primaryKeys
     *            主键信息，如果是复合主键，要把所有主键都带上
     */
    abstract public function fetchOne($tableName, $primaryKeys);

    /**
     * 根据select条件查询所有记录
     *
     * @param Select $select            
     */
    abstract public function fetchAll($select);

    /**
     * 转义字符
     *
     * @param string $value            
     * @return string
     */
    abstract public function quoteStr($value);
}
