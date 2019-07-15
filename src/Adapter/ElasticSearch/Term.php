<?php
namespace Nosql\Adapter\ElasticSearch;
/**
 * 查询条件
 * @author roy
 *
 */
abstract class Term
{

    /**
     * 子查询
     * @var array
     */
    public $child = [];
    /**
     * 转化为数组
     */
    abstract function toArray();
    
    /**
     * 添加子查询
     * @param BoolTerm $term
     */
    public function addChild($boolTerm){
        $this->child[] = $boolTerm;
    }
}