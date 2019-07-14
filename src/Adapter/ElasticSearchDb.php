<?php
namespace Nosql\Adapter;

use Nosql\Select\ElasticSearchSelect;
use Nosql\Adapter\AbstractDb;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Nosql\DbException;

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
                    'host' => $this->config ["host"],
                    'port' => $this->config ["port"] 
                ] 
            ];
            $builder->setHosts ( $hosts );
            
            $builder->setRetries ( isset ( $this->config ["retries"] ) ? $this->config ["retries"] : 2 );
            if (isset ( $this->config ["logfile"] )) {
                $logger = new Logger ( 'elasticsearch' );
                $logger->pushHandler ( new StreamHandler ( $this->config ["logfile"], Logger::WARNING ) );
                $builder->setLogger ( $logger );
            }
            $this->client = $builder->build ();
        }else{
            throw new DbException("配置错误");
        }
    }

    /**
     * 检查必须的配置
     *
     * @return boolean
     */
    private function checkRequiredConfig()
    {
        if (! key_exists ( "host", $this->config )) {
            return false;
        }
        if (! key_exists ( "port", $this->config )) {
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
        $this->connect ();
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
        $this->connect ();
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
        $this->connect ();
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
        $this->connect ();
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
        $this->connect ();
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
     * @return string
     */
    public function quoteStr($value)
    {
    	if (is_numeric( $value )) {
    		return $value;
    	}
    	return "'" . addcslashes ( $value, "\000\n\r\\'\"\032" ) . "'";
    }

    /**
     * {@inheritDoc}
     * @see \Nosql\Adapter\AbstractDb::batchSave()
     */
    public function batchSave($tableName, $items)
    {
        $this->connect ();
        // TODO Auto-generated method stub
        
    }

    /**
     * {@inheritDoc}
     * @see \Nosql\Adapter\AbstractDb::batchDelete()
     */
    public function batchDelete($tableName, $ids)
    {
        $this->connect ();
        // TODO Auto-generated method stub
        
    }

}