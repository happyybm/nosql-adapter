<?php
namespace Nosql\Adapter;

require "../../vendor/autoload.php";

use Nosql\Select\ElasticSearchSelect;
use Nosql\Adapter\AbstractDb;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

/**
 * ElasticSearch适配器
 */
class ElasticSearchDb extends AbstractDb
{
    
    /**
     * Elasticsearch\Client
     *
     * @var Client
     */
    protected $client = null;

    /**
     * 返回select对象
     *
     * @return ElasticSearchSelect
     */
    public function select()
    {
        return new ElasticSearchSelect ( $this );
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
            $builder = ClientBuilder::create ();
            $hosts = [ 
                [ 
                    'host' => $this->_config ["host"],
                    'port' => $this->_config ["port"] 
                ] 
            ];
            $builder->setHosts ( $hosts );
            
            $builder->setRetries ( isset ( $this->_config ["retries"] ) ? $this->_config ["retries"] : 2 );
            if (isset ( $this->_config ["logfile"] )) {
                $logger = new Logger ( 'elasticsearch' );
                $logger->pushHandler ( new StreamHandler ( $this->_config ["logfile"], Logger::WARNING ) );
                $builder->setLogger ( $logger );
            }
            $this->client = $builder->build ();
        }
    }

    /**
     * 检查必须的配置
     *
     * @return boolean
     */
    private function checkRequiredConfig()
    {
        if (! key_exists ( "host", $this->_config )) {
            return false;
        }
        if (! key_exists ( "port", $this->_config )) {
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
     *
     * {@inheritdoc}
     *
     * @see \Nosql\Adapter\AbstractDb::getConnection()
     */
    public function getConnection()
    {
        $this->connect ();
        return $this->client;
    }

    /**
     * 根据主键查询唯一一条item
     *
     * @param string $indexName
     *            索引名
     * @param string $keyValue 主键id
     * @return array 返回的数据
     * @throws ElasticSearchException
     */
    public function fetchOne($indexName, $keyValue)
    {
        return $this->getItem ( $indexName, $keyValue );
    }

    /**
     * 根据查询条件返回所有记录
     *
     * @param ElasticSearchSelect $select            
     */
    public function fetchAll($select)
    {
        $query = $select->getQuery ();
        return $this->search( $query ["Params"] );
    }

    /**
     * 保存或更新主键相同的item
     *
     * @param string $indexName
     *            索引名
     * @param array $data
     *            要保存或更新的item
     */
    public function save($indexName, $data)
    {
        return $this->putItem ( $indexName, $data );
    }

    /**
     * 更新一条item
     *
     * @param string $indexName
     *            索引名
     * @param array $data
     *            要保存或更新的item
     * @param array $ids
     *            主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
     * @return boolean
     */
    public function update($indexName, $data, $ids)
    {
        return $this->updateItem ( $indexName, $data, $ids );
    }

    /**
     * 根据主键删除记录
     *
     * @param string $indexName
     *            索引名
     * @param array $ids
     *            主键，如果是复合主键，要把所有主键都带上，主键内容是不能更新的，即使在data参数里设置了。
     */
    public function delete($indexName, $ids)
    {
        return $this->deleteItem ( $indexName, $ids );
    }

    /**
     * 删除item
     *
     * @param string $indexName            
     * @param array $keyValues            
     */
    private function deleteItem($indexName, $keyValues)
    {
        foreach ($keyValues as $keyValue){
            $params = [
                'index' => $indexName,
                'id'    => $keyValue
            ];
            $r = $this->client->delete($params);
        }
        return  $r;
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
    private function getItem($indexName, $keyValue)
    {
        $params = [
            'index' => $indexName,
            'id'    => $keyValue
        ];
        return $this->client->get($params);
    }

    /**
     * 保存新item
     *
     * @param string $indexName
     *            索引名
     * @param array $data
     *            要保存的item
     * @param string $keyName
     *            主键名称
     */
    private function putItem($indexName, $data)
    {
        $params = [
            'index' => $indexName,
            'body'  => $data
        ];
        $response = $this->client->index($params);
    }

    /**
     * 查询
     * @param array $params
     * @return callable|array
     */
    private function search($params)
    {
        return $this->client->search($params);
    }

    /**
     * 更新或增加一条新item
     *
     * @param string $indexName            
     * @param array $data
     *            要更新的属性
     * @param array $keys
     *            指定主键id
     */
    private function updateItem($indexName, $data, $keys)
    {
        foreach ($keys as $key){
            $params = [
                'index' => $indexName,
                'id'    => $key,
                'body'  => [
                    'doc' => $data,
                ]
            ];
            $response = $this->client->update($params);
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
     *            例：Session=:param1 And Session<>:param2 其中param1，跟param2就是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("param1"=>"sdfsdfsd","param2"=>"sdfsdfsdf")
     */
    public function quote($cond, $bind)
    {
        foreach ( $bind as $key => $v ) {
            $v = $this->quoteStr ( $v );
            $cond = preg_replace ( "/:" . $key . "/", $v, $cond );
        }
        return $cond;
    }
}