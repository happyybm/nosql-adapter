<?php
namespace Nosql\Select;

use Nosql\Select\Select;

class ElasticSearchSelect extends Select
{
    /**
     * 多个索引同时搜索
     * @var array
     */
    private $indexs = [];

    /*
     * (non-PHPdoc)
     * @see \Duomai\Db\Select\Select::from()
     */
    public function from($indexName)
    {
        if(!in_array($indexName, $this->indexs)){
            $this->indexs[] = $indexName;
        }
        return $this;
    }

    /**
     * 根据不同的条件，指定使用的查询方式：Query,Scan
     */
    protected function assemble()
    {
        
    }
    
    

    /*
     * (non-PHPdoc) @see \Duomai\Db\Select\Select::checkCond()
     */
    protected function checkCond()
    {
        return true;
    }
}